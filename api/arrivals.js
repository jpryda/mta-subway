// Vercel Serverless Function: MTA GTFS-RT → JSON (no API key)
// Grouped by station; multiple stations; prefix stop_id match; detailed debug for "no time" matches.

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

// ✅ Correct GTFS-RT field numbers (notably stop_id = 4)
const SCHEMA = `
  syntax = "proto2";
  package transit_realtime;

  message FeedMessage {
    required FeedHeader header = 1;
    repeated FeedEntity entity = 2;
  }

  message FeedHeader {
    required string gtfs_realtime_version = 1;
    optional int64 timestamp = 2;
  }

  message FeedEntity {
    required string id = 1;
    optional TripUpdate trip_update = 3;
  }

  message TripUpdate {
    optional TripDescriptor trip = 1;
    repeated StopTimeUpdate stop_time_update = 2;
  }

  message TripDescriptor {
    optional string route_id = 5;
  }

  message StopTimeUpdate {
    optional uint32 stop_sequence = 1;
    optional Event arrival = 2;
    optional Event departure = 3;
    optional string stop_id = 4; // 👈 correct field number
  }

  message Event {
    optional int64 time = 1;
    // optional int32 delay = 2;
    // optional int32 uncertainty = 3;
  }
`;
const root = protobuf.parse(SCHEMA).root;
const FeedMessage = root.lookupType("transit_realtime.FeedMessage");

const norm = s => (s || "").toLowerCase().replace(/street\b/g, "st").replace(/\s+/g, " ").trim();

// ✅ Station mapping
const STATION_TO_STOP_IDS = {
  // Clark St (2/3)
  "clark st": ["231N", "231S"],
  "clark street": ["231N", "231S"],

  // High St (A/C) — High St uses A40* in static; C shows via routeId
  "high st": ["A40N", "A40S"],
  "high street": ["A40N", "A40S"]
};

function prefixMatch(sid, ids) {
  return !!sid && ids.some(id => sid.startsWith(id));
}

async function fetchFeed(path) {
  try {
    const r = await fetch(API_BASE + path, {
      headers: {
        "Accept": "application/x-protobuf",
        "User-Agent": "mta-arrivals/1.6 (+vercel)"
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
    const stationRaw = url.searchParams.get("station");     // comma-separated
    const stopIdsParam = url.searchParams.get("stop_ids");  // comma-separated
    const maxPerRoute = Math.max(1, Math.min(10, Number(url.searchParams.get("max_per_route") || 5)));
    const windowSeconds = Number(url.searchParams.get("window_seconds") || 0); // 0 = future-only
    const showDebug = url.searchParams.get("show_debug") === "1";

    // Resolve stations -> stop_ids (support multiple stations)
    const stationsResolved = [];
    const unknownStations = [];
    const stopIdsSet = new Set();
    const baseToStation = new Map(); // map base stop_id prefix to station name

    if (stationRaw) {
      for (const name of stationRaw.split(",").map(s => s.trim()).filter(Boolean)) {
        const key = norm(name);
        const mapped = STATION_TO_STOP_IDS[key];
        if (mapped?.length) {
          stationsResolved.push(name);
          for (const id of mapped) {
            stopIdsSet.add(id);
            baseToStation.set(id, name);
          }
        } else {
          unknownStations.push(name);
        }
      }
    }

    if (stopIdsParam) {
      for (const id of stopIdsParam.split(",").map(s => s.trim()).filter(Boolean)) {
        stopIdsSet.add(id);
        // if not in baseToStation, it will fall under "Unknown" group
      }
    }

    const stopIds = Array.from(stopIdsSet);
    if (!stopIds.length) {
      return res.status(400).json({
        error: "No stop_ids resolved. Provide station=Clark St[,High St] and/or stop_ids=A40N,..."
      });
    }

    const now = Math.floor(Date.now() / 1000);
    let matchedNoTime = 0, matchedWithTime = 0;
    let totalEntities = 0, totalTripUpdates = 0, totalStopTimeUpdates = 0;

    const sampleSet = new Set();
    const matchedNoTimeSet = new Set();
    const matchedNoTimeDetails = []; // ✅ capture detail for no-time matches

    const stationsObj = {};

    function pushArrival(sid, route, ts) {
      // map sid back to a station display name via base prefix
      let stationName = "Unknown";
      for (const [base, name] of baseToStation.entries()) {
        if (sid.startsWith(base)) { stationName = name; break; }
      }
      const s = (stationsObj[stationName] ??= {});
      const r = (s[route] ??= []);
      const entry = {
        stop_id: sid,
        arrival_epoch: ts ?? null,
        in_min: ts == null ? null : Math.max(0, Math.round((ts - now) / 60))
      };
      if (ts == null) entry.status = "approaching";
      r.push(entry);
    }

    function maybeAddSample(sid) {
      if (sampleSet.size < 50 && sid) sampleSet.add(sid);
    }

    // Fetch feeds and collect matches
    for (const f of FEEDS) {
      const msg = await fetchFeed(f);
      if (!msg) continue;

      totalEntities += msg.entity.length;

      for (const e of msg.entity) {
        const tu = e.tripUpdate; // camelCase
        if (!tu) continue;

        totalTripUpdates++;

        for (const stu of tu.stopTimeUpdate || []) { // camelCase
          totalStopTimeUpdates++;

          const sid = stu.stopId; // ✅ populated via correct schema
          maybeAddSample(sid);

          if (!prefixMatch(sid, stopIds)) continue;

          const route = tu.trip?.routeId || "?";
          const ts = Number(stu.arrival?.time || stu.departure?.time || 0);

          if (!ts) {
            matchedNoTime++;
            matchedNoTimeSet.add(sid);
            matchedNoTimeDetails.push({
              stop_id: sid,
              route,
              arrival: stu.arrival || null,
              departure: stu.departure || null
            });
            // surface as "approaching"
            pushArrival(sid, route, null);
            continue;
          }

          // Time filter
          if (windowSeconds === 0) {
            if (ts < now) continue;               // future only
          } else {
            if (ts < now - windowSeconds) continue; // allow slight lookback
          }

          matchedWithTime++;
          pushArrival(sid, route, ts);
        }
      }
    }

    // Sort & trim each station/route
    for (const [stationName, routes] of Object.entries(stationsObj)) {
      for (const [route, arrs] of Object.entries(routes)) {
        arrs.sort((a, b) => (a.arrival_epoch ?? Infinity) - (b.arrival_epoch ?? Infinity));
        routes[route] = arrs.slice(0, maxPerRoute);
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
      stations: stationsObj
    };

    if (showDebug) {
      payload.debug = {
        matched_no_time: matchedNoTime,
        matched_with_time: matchedWithTime,
        total_entities: totalEntities,
        total_trip_updates: totalTripUpdates,
        total_stop_time_updates: totalStopTimeUpdates,
        sample_stop_ids: Array.from(sampleSet),
        matched_no_time_ids: Array.from(matchedNoTimeSet),
        matched_no_time_details: matchedNoTimeDetails
      };
    }

    res.setHeader("Cache-Control", "s-maxage=15, stale-while-revalidate=30");
    return res.status(200).json(payload);

  } catch (err) {
    return res.status(500).json({ error: err?.message || "server error" });
  }
}
