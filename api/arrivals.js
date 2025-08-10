// Vercel Serverless Function: Keyless MTA GTFS-RT → JSON arrivals
// Usage examples:
//   /api/arrivals?station=Clark%20St
//   /api/arrivals?station=High%20St
//   /api/arrivals?stop_ids=A41N,A41S,C41N,C41S
// Optional: &max_per_route=5

import protobuf from "protobufjs";

// Official GTFS-RT feed groups (no API key needed)
const FEEDS = [
  "nyct%2Fgtfs-ace",
  "nyct%2Fgtfs-bdfm",
  "nyct%2Fgtfs-g",
  "nyct%2Fgtfs-jz",
  "nyct%2Fgtfs-l",
  "nyct%2Fgtfs-nqrw",
  "nyct%2Fgtfs-7",
  "nyct%2Fgtfs",     // 1/2/3/4/5/6 lines
  "nyct%2Fgtfs-si"   // Staten Island
];

const API_BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/";

// Minimal inline GTFS-RT schema
let rootPromise;
async function loadSchema() {
  if (!rootPromise) {
    rootPromise = protobuf.load(`
      syntax = "proto2";
      package transit_realtime;
      message FeedMessage { required FeedHeader header = 1; repeated FeedEntity entity = 2; }
      message FeedHeader { required string gtfs_realtime_version = 1; optional int64 timestamp = 2; }
      message FeedEntity { required string id = 1; optional TripUpdate trip_update = 3; }
      message TripUpdate {
        optional TripDescriptor trip = 1;
        repeated StopTimeUpdate stop_time_update = 2;
      }
      message TripDescriptor { optional string route_id = 5; }
      message StopTimeUpdate {
        optional string stop_id = 1;
        optional Event arrival = 2;
        optional Event departure = 3;
      }
      message Event { optional int64 time = 1; }
    `);
  }
  return rootPromise;
}

// Handy normalization for a couple of your stations
function norm(s) {
  return (s || "").toLowerCase().replace(/street\b/g, "st").replace(/\s+/g, " ").trim();
}

// Pre-mapped stop_ids for your two stations.
// (You can extend this map later as you add favorites.)
const STATION_TO_STOP_IDS = {
  // Clark St (2/3)
  "clark st": ["R23N", "R23S", "R33N", "R33S"],
  "clark street": ["R23N", "R23S", "R33N", "R33S"],
  // High St (A/C)
  "high st": ["A41N", "A41S", "C41N", "C41S"],
  "high street": ["A41N", "A41S", "C41N", "C41S"],
};

export default async function handler(req, res) {
  // CORS: allow use from Shortcuts or browser
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
      const key = norm(stationParam);
      const mapped = STATION_TO_STOP_IDS[key];
      if (!mapped) {
        return res.status(400).json({
          error: `Unknown station "${stationParam}". For now this endpoint supports Clark St and High St, or pass stop_ids explicitly.`
        });
      }
      stopIds = mapped;
    } else {
      return res.status(400).json({ error: "Provide ?station=Clark St|High St or ?stop_ids=A41N,..." });
    }

    const now = Math.floor(Date.now() / 1000);
    const schema = await loadSchema();
    const FeedMessage = schema.lookupType("transit_realtime.FeedMessage");

    // Pull and parse all feeds
    const arrivals = [];
    for (const f of FEEDS) {
      try {
        const r = await fetch(`${API_BASE}${f}`);
        if (!r.ok) continue;
        const buf = Buffer.from(await r.arrayBuffer());
        const msg = FeedMessage.decode(buf);

        for (const ent of msg.entity) {
          const tu = ent.trip_update;
          if (!tu) continue;
          const route = tu.trip?.route_id || "?";

          for (const stu of tu.stop_time_update || []) {
            const sid = stu.stop_id;
            if (!sid || !stopIds.includes(sid)) continue;

            const ts = Number(stu.arrival?.time || stu.departure?.time || 0);
            if (!ts || ts < now - 15) continue;

            arrivals.push({
              stop_id: sid,
              route,
              arrival_epoch: ts,
              in_min: Math.max(0, Math.round((ts - now) / 60))
            });
          }
        }
      } catch (_e) {
        // Ignore a failing feed; others will still return data
      }
    }

    // Sort and group by route, trimming to maxPerRoute
    arrivals.sort((a, b) => a.arrival_epoch - b.arrival_epoch);
    const byRoute = {};
    for (const item of arrivals) {
      (byRoute[item.route] ??= []);
      if (byRoute[item.route].length < maxPerRoute) byRoute[item.route].push(item);
    }

    // Helpful meta
    const meta = {
      station: stationParam || null,
      stop_ids: stopIds,
      generated_at: now,
      max_per_route: maxPerRoute
    };

    // Cache control: okay to cache briefly at the edge
    res.setHeader("Cache-Control", "s-maxage=15, stale-while-revalidate=30");

    return res.status(200).json({ meta, routes: byRoute });
  } catch (err) {
    return res.status(500).json({ error: err?.message || "server error" });
  }
}

