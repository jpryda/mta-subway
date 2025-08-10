// api/arrivals_by_station.js
import fetch from 'node-fetch';
import protobuf from 'protobufjs';
import { Buffer } from 'node:buffer';

const FEEDS = [
  'nyct%2Fgtfs-ace','nyct%2Fgtfs-bdfm','nyct%2Fgtfs-g','nyct%2Fgtfs-jz',
  'nyct%2Fgtfs-l','nyct%2Fgtfs-nqrw','nyct%2Fgtfs-7','nyct%2Fgtfs','nyct%2Fgtfs-si'
];
const API_BASE = 'https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/';
const STATIONS_API = 'https://data.ny.gov/resource/5f5g-n3cz.json'; // Stations & Complexes (GTFS Stop IDs) 

let rootPromise;
async function loadSchema() {
  if (!rootPromise) {
    rootPromise = protobuf.load(`
      syntax = "proto2";
      package transit_realtime;
      message FeedMessage { required FeedHeader header = 1; repeated FeedEntity entity = 2; }
      message FeedHeader { required string gtfs_realtime_version = 1; optional int64 timestamp = 2; }
      message FeedEntity { required string id = 1; optional TripUpdate trip_update = 3; }
      message TripUpdate { optional TripDescriptor trip = 1; repeated StopTimeUpdate stop_time_update = 2; }
      message TripDescriptor { optional string route_id = 5; }
      message StopTimeUpdate { optional string stop_id = 1; optional Event arrival = 2; optional Event departure = 3; }
      message Event { optional int64 time = 1; }
    `);
  }
  return rootPromise;
}

// best-effort normalize names: "Clark St", "Clark Street", "High St", "High Street"
function normalize(s) {
  return (s || '').toLowerCase().replace(/street\b/g,'st').replace(/\s+/g,' ').trim();
}

async function lookupStopIds(station) {
  const q = normalize(station);
  // Query station + complex names; Socrata API supports case-insensitive like;
  const url = `${STATIONS_API}?$select=station_complex,station_name,gtfs_stop_id&$limit=50`;
  const res = await fetch(url);
  const rows = await res.json();
  // Filter to rows whose displayed name matches our normalized query
  const hits = rows.filter(r => {
    const a = normalize(r.station_name);
    const b = normalize(r.station_complex || '');
    return a === q || b === q;
  });
  // Fallback: partial match
  const pool = hits.length ? hits : rows.filter(r => {
    const a = normalize(r.station_name);
    const b = normalize(r.station_complex || '');
    return a.includes(q) || b.includes(q);
  });
  // Collect stop_ids (semicolon- or comma-separated in field, depending on dataset row)
  const ids = new Set();
  for (const r of pool) {
    const raw = (r.gtfs_stop_id || '').toString();
    for (const piece of raw.split(/[;,]/)) {
      const v = piece.trim();
      if (v) ids.add(v);
    }
  }
  if (!ids.size) throw new Error(`No GTFS stop_ids found for "${station}"`);
  return [...ids];
}

export default async function handler(req, res) {
  try {
    const station = (req.query.station || '').trim();
    if (!station) return res.status(400).json({ error: 'station required (e.g., Clark St, High St)' });

    const stopIds = await lookupStopIds(station);

    const headers = { 'x-api-key': process.env.MTA_API_KEY };
    const schema = await loadSchema();
    const FeedMessage = schema.lookupType('transit_realtime.FeedMessage');
    const now = Math.floor(Date.now() / 1000);
    const arrivals = [];

    // Pull each line group feed (official MTA GTFS-RT) and parse protobuf
    for (const f of FEEDS) {
      const r = await fetch(`${API_BASE}${f}`, { headers });
      if (!r.ok) continue;
      const buf = Buffer.from(await r.arrayBuffer());
      const msg = FeedMessage.decode(buf);
      for (const ent of msg.entity) {
        const tu = ent.trip_update;
        if (!tu) continue;
        const route = tu.trip?.route_id || '?';
        for (const stu of tu.stop_time_update || []) {
          const sid = stu.stop_id;
          if (!sid || !stopIds.includes(sid)) continue;
          const t = Number(stu.arrival?.time || stu.departure?.time || 0);
          if (t && t >= now - 15) {
            arrivals.push({
              stop_id: sid, route,
              arrival_epoch: t,
              in_min: Math.max(0, Math.round((t - now) / 60))
            });
          }
        }
      }
    }

    // Sort, group, and trim
    arrivals.sort((a,b)=>a.arrival_epoch-b.arrival_epoch);
    const byRoute = {};
    for (const a of arrivals) {
      byRoute[a.route] ??= [];
      if (byRoute[a.route].length < 5) byRoute[a.route].push(a);
    }

    res.setHeader('Cache-Control', 's-maxage=15, stale-while-revalidate=30');
    return res.status(200).json({ station, generated_at: now, routes: byRoute });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: e.message || 'server error' });
  }
}

