// Vercel Serverless Function: MTA GTFS-RT → JSON (no API key)
// Fixes: protobuf.parse, camelCase fields, prefix stop_id match, future-only filter, debug counters

import protobuf from "protobufjs";

const FEEDS = [
  "nyct%2Fgtfs-ace",
  "nyct%2Fgtfs-bdfm",
  "nyct%2Fgtfs-g",
  "nyct%2Fgtfs-jz",
  "nyct%2Fgtfs-l",
  "nyct%2Fgtfs-nqrw",
  "nyct%2Fgtfs-7",
  "nyct%2Fgtfs",     // 1/2/3/4/5/6
  "nyct%2Fgtfs-si"   // Staten Island
];
const API_BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/";

// Minimal GTFS-RT schema
const SCHEMA = `
  syntax = "proto2";
  package transit_realtime;
  message FeedMessage { required FeedHeader header = 1; repeated FeedEntity entity = 2; }
  message FeedHeader { required string gtfs_realtime_version = 1; optional int64 timestamp = 2; }
  message FeedEntity { required string id = 1; optional TripUpdate trip_update = 3; }
  message TripUpdate { optional TripDescriptor trip = 1; repeated StopTimeUpdate stop_time_update = 2; }
  message TripDescriptor { optional string route_id = 5; }
  message StopTimeUpdate { optional string stop_id = 1; optional Event arrival = 2; optional Event departure = 3; }
  message Event { optional int64 time = 1; }
`;
const root = protobuf.parse(SCHEMA).root;
const FeedMessage = root.lookupType("transit_realtime.FeedMessage");

// Utilities
const norm = s => (s || "").toLowerCase().replace(/street\b/g, "st").replace(/\s+/g, " ").trim();

const STATION_TO_STOP_IDS = {
  "clark st": ["R23N", "R23S", "R33N", "R33S"],
  "clark street": ["R23N", "R23S", "R33N", "R33S"],
  "high st": ["A41N", "A41S", "C41N", "C41S"],
  "high street": ["A41N", "A41S", "C41N", "C41S"]
};

function prefixMatch(sid, ids) {
  return !!sid && ids.some(id => sid.startsWith(id));
}

async function fetchFeed(path) {
  try {
    const r = await fetch(API_BASE + path, {
      headers: {
        "Accept": "application/x-protobuf",
        "User-Agent": "mta-arrivals/1.0 (+vercel)"
      }
    });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    const buf = new Uint8Array(await r.arrayBuffer());
    return FeedMessage.decode(buf);
  } catch (e) {
    return null; // skip invalid feeds
  }
}

export default async function handler(req, res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(204).end();

  try {
    const url = new URL(req.url, "http://localhost");
    const stationParam = url.searchParams.get("station");
    const stopIdsParam = url.searchParams.get("stop_ids");
    const maxPerRoute = Math.max(1, Math.min(10, Number(url.searchParams.get("max_per_route") || 5)));

    let stopIds = [];
    if (stopIdsParam) {
      stopIds = stopIdsParam.split(",").map(s => s.trim()).filter(Boolean);
    } else if (stationParam) {
      const mapped = STATION_TO_STOP_IDS[norm(stationParam)];
      if (!mapped) {
        return res.status(400).json({ error: `Unknown station "${stationParam}". Use Clark St / High St, or pass stop_ids=` });
      }
      stopIds = mapped;
    } else {
      return res.status(400).json({ error: "Provide ?station=Clark St|High St or ?stop_ids=A41N,..." });
    }

    const now = Math.floor(Date.now() / 1000);
    const windowSeconds = Number(url.searchParams.get("window_seconds") || 0); 
    const showDebug = url.searchParams.get("show_debug") === "1";

    const results = [];
    let matchedNoTime = 0;
    let matchedWithTime = 0;

    for (const f of FEEDS) {
      const msg = await fetchFeed(f);
      if (!msg) continue;

      for (const e of msg.entity) {
        const tu = e.tripUpdate; // camelCase
        if (!tu) continue;

        const route = tu.trip?.routeId || "?"; // camelCase

        for (const stu of tu.stopTimeUpdate || []) { // camelCase
          const sid = stu.stopId; // camelCase
          if (!prefixMatch(sid, stopIds)) continue;

          const ts = Number(stu.arrival?.time || stu.departure?.time || 0);
          if (!ts) { matchedNoTime++; continue; }

          if (windowSeconds === 0) {
            if (ts < now) continue;
          } else {
            if (ts < now - windowSeconds) continue;
          }

          matchedWithTime++;
          results.push({ route, stop_id: sid, arrival_epoch: ts });
        }
      }
    }

    results.sort((a, b) => a.arrival_epoch - b.arrival_epoch);
    const byRoute = {};
    for (const r of results) {
      (byRoute[r.route] ??= []);
      if (byRoute[r.route].length < maxPerRoute) {
        const in_min = Math.max(0, Math.round((r.arrival_epoch - now) / 60));
        byRoute[r.route].push({ stop_id: r.stop_id, arrival_epoch: r.arrival_epoch, in_min });
      }
    }

    const payload = {
      meta: { station: stationParam || null, stop_ids: stopIds, generated_at: now, max_per_route: maxPerRoute },
      routes: byRoute
    };
    if (showDebug) {
      payload.debug = {
        matched_no_time: matchedNoTime,
        matched_with_time: matchedWithTime,
        window_seconds: windowSeconds
      };
    }

    res.setHeader("Cache-Control", "s-maxage=15, stale-while-revalidate=30");
    return res.status(200).json(payload);

  } catch (err) {
    return res.status(500).json({ error: err?.message || "server error" });
  }
}

