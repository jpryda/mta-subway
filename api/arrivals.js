// Vercel Serverless Function: MTA GTFS-RT → JSON (no API key)
// Multi-station support, camelCase fixes, prefix stop_id match, time window + debug

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

// Minimal GTFS-RT schema (proto2), parsed in-memory
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
  // Clark St (2/3)
  "clark st": ["R23N", "R23S", "R33N", "R33S"],
  "clark street": ["R23N", "R23S", "R33N", "R33S"],
  // High St (A/C)
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
        "User-Agent": "mta-arrivals/1.1 (+vercel)"
      }
    });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    const buf = new Uint8Array(await r.arrayBuffer());
    return FeedMessage.decode(buf);
  } catch {
    return null; // tolerate transient non-protobuf responses
  }
}

export default async function handler(req, res) {
  // CORS
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(204).end();

  try {
    const url = new URL(req.url, "http://localhost");

    // Inputs
    const stationRaw = url.searchParams.get("station");     // may be comma-separated
    const stopIdsParam = url.searchParams.get("stop_ids");  // may be comma-separated
    const maxPerRoute = Math.max(1, Math.min(10, Number(url.searchParams.get("max_per_route") || 5)));
    const windowSeconds = Number(url.searchParams.get("window_seconds") || 0); // 0=future-only
    const showDebug = url.searchParams.get("show_debug") === "1";

    // Resolve stations -> stop_ids, allow multiple stations
    const stationsResolved = [];
    const unknownStations = [];
    const stopIdsSet = new Set();

    if (stationRaw) {
      const names = stationRaw.split(",").map(s => s.trim()).filter(Boolean);
      for (const name of names) {
        const key = norm(name);
        const mapped = STATION_TO_STOP_IDS[key];
        if (mapped && mapped.length) {
          stationsResolved.push(name);
          for (const id of mapped) stopIdsSet.add(id);
        } else {
          unknownStations.push(name);
        }
      }
    }

    if (stopIdsParam) {
      for (const id of stopIdsParam.split(",").map(s => s.trim()).filter(Boolean)) {
        stopIdsSet.add(id);
      }
    }

    // Validate we have at least one stop_id
    const stopIds = Array.from(stopIdsSet);
    if (!stopIds.length) {
      return res.status(400).json({
        error: "No stop_ids resolved. Provide station=Clark St[,High St] and/or stop_ids=A41N,..."
      });
    }

    const now = Math.floor(Date.now() / 1000);
    const results = [];
    let matchedNoTime = 0;
    let matchedWithTime = 0;

    // Fetch feeds and collect matches
    for (const f of FEEDS) {
      const msg = await fetchFeed(f);
      if (!msg) continue;

      for (const e of msg.entity) {
        const tu = e.tripUpdate;
        if (!tu) continue;

        const route = tu.trip?.routeId || "?";

        for (const stu of tu.stopTimeUpdate || []) {
          const sid = stu.stopId;
          if (!prefixMatch(sid, stopIds)) continue;

          const ts = Number(stu.arrival?.time || stu.departure?.time || 0);
          if (!ts) { matchedNoTime++; continue; }

          // Time filter: future-only if windowSeconds==0, else allow slight past
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

    // Sort and group by route
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
      meta: {
        stations: stationsResolved.length ? stationsResolved : null,
        unknown_stations: unknownStations.length ? unknownStations : null,
        stop_ids: stopIds,
        generated_at: now,
        max_per_route: maxPerRoute,
        window_seconds: windowSeconds
      },
      routes: byRoute
    };

    if (showDebug) {
      payload.debug = {
        matched_no_time: matchedNoTime,
        matched_with_time: matchedWithTime
      };
    }

    res.setHeader("Cache-Control", "s-maxage=15, stale-while-revalidate=30");
    return res.status(200).json(payload);

  } catch (err) {
    return res.status(500).json({ error: err?.message || "server error" });
  }
}

