// Vercel Serverless Function: MTA GTFS-RT → JSON (no API key)
// - Grouped by station; multi-station; base stop_id match (N/S kept on items).
// - Optional filtering by routes via ?routes=A,C,1 (works with or without station).
// - NEW: route_direction=N|S|BOTH filters **data** (JSON + speech). Default BOTH.
// - format=speech supports speech_limit (default 2).
// - Epoch-in-delay fallback is ALWAYS ON (treatDelayAsEpoch = true).
// - Debug: show_debug=1 (with optional raw_dump=1) returns interpreter details.

import protobuf from "protobufjs";
// Load name→base stop_id[] index (minified JSON) and normalize keys once
import stationIndexRaw from "../data/station_index.min.json" with { type: "json" };

const FEEDS = [
  "nyct%2Fgtfs-ace",
  "nyct%2Fgtfs-bdfm",
  "nyct%2Fgtfs-g",
  "nyct%2Fgtfs-jz",
  "nyct%2Fgtfs-l",
  "nyct%2Fgtfs-nqrw",
  "nyct%2Fgtfs-7",
  "nyct%2Fgtfs",
  "nyct%2Fgtfs-si"
];
const API_BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/";
const EPOCH_GUESS_THRESHOLD = 1104537600; // 2005-01-01
const TREAT_DELAY_AS_EPOCH = true;        // <-- hardcoded ON

// Minimal GTFS-RT schema (TripUpdate + StopTimeUpdate + Event)
const SCHEMA = `
syntax = "proto2"; package transit_realtime;
message FeedMessage { required FeedHeader header = 1; repeated FeedEntity entity = 2; }
message FeedHeader { required string gtfs_realtime_version = 1; optional int64 timestamp = 2; }
message FeedEntity { required string id = 1; optional TripUpdate trip_update = 3; }
message TripUpdate { optional TripDescriptor trip = 1; repeated StopTimeUpdate stop_time_update = 2; }
message TripDescriptor { optional string route_id = 5; }
message StopTimeUpdate {
  optional uint32 stop_sequence = 1;
  optional Event arrival = 2;
  optional Event departure = 3;
  optional string stop_id = 4;
}
message Event { optional int64 time = 1; optional int32 delay = 2; optional int32 uncertainty = 3; }
`;
const FeedMessage = protobuf.parse(SCHEMA).root.lookupType("transit_realtime.FeedMessage");

// Utilities
const norm = s => (s || "").toLowerCase().replace(/street\b/g, "st").replace(/\s+/g, " ").trim();
const dirFromStopId = sid => (sid && /[NS]$/.test(sid) ? sid.slice(-1) : null);
const asIso = sec => (sec == null ? null : new Date(Number(sec) * 1000).toISOString());
// Kept for reference
const prefixMatch = (sid, ids) => !!sid && ids.some(id => sid.startsWith(id));
// Base stop_id helper for O(1) membership checks
const baseOf = sid => (sid && (sid.endsWith("N") || sid.endsWith("S"))) ? sid.slice(0, -1) : sid;

// Build normalized index: Map<normalizedStationName, string[] of base stop_ids>
const STATION_INDEX = new Map(
  Object.entries(stationIndexRaw).map(([k, v]) => [
    norm(k),
    (Array.isArray(v) ? v : [v]).map(String)
  ])
);

async function fetchFeed(path) {
  try {
    const r = await fetch(API_BASE + path, {
      headers: { "Accept": "application/x-protobuf", "User-Agent": "mta-arrivals/2.5 (+vercel)" }
    });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    return FeedMessage.decode(new Uint8Array(await r.arrayBuffer()));
  } catch {
    return null;
  }
}

function parseQuery(req) {
  const url = new URL(req.url, "http://localhost");
  const qp = k => url.searchParams.get(k);
  const num = (k, d) => Number(qp(k) ?? d);
  const bool = k => qp(k) === "1";

  const stationRaw = qp("station");
  const stopIdsParam = qp("stop_ids");
  const routesParam = qp("routes"); // e.g., "A,C,1"
  const routesFilter = routesParam
    ? routesParam.split(",").map(s => s.trim()).filter(Boolean).map(s => s.toUpperCase())
    : null;

  const maxPerRoute = Math.max(1, Math.min(10, num("max_per_route", 5)));
  const lookbackSeconds = num("lookback_seconds", 0); // 0=future-only
  const showDebug = bool("show_debug");
  const rawDump = bool("raw_dump");
  const format = qp("format"); // "speech" optional
  const speechLimit = Math.max(1, num("speech_limit", 2));
  const routeDirection = (qp("route_direction") || "BOTH").toUpperCase(); // N|S|BOTH

  return {
    stationRaw, stopIdsParam, routesParam, routesFilter,
    maxPerRoute, lookbackSeconds, showDebug, rawDump, format, speechLimit, routeDirection
  };
}

function resolveStops(stationRaw, stopIdsParam) {
  const stationsResolved = [];
  const unknownStations = [];
  const stopIdsSet = new Set();     // base stop_ids (no N/S)
  const baseToStation = new Map();  // base stop_id -> display name (per-request inverse)

  if (stationRaw) {
    for (const name of stationRaw.split(",").map(s => s.trim()).filter(Boolean)) {
      const key = norm(name);
      const mapped = STATION_INDEX.get(key);
      if (mapped?.length) {
        stationsResolved.push(name);
        for (const id of mapped) { stopIdsSet.add(id); baseToStation.set(id, name); }
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

  // Return both for debug + hot-path checks
  return { stationsResolved, unknownStations, stopIds: Array.from(stopIdsSet), stopIdsSet, baseToStation };
}

function getTimes(stu) {
  const aTime = stu.arrival?.time ?? null;
  const dTime = stu.departure?.time ?? null;
  const aDelay = stu.arrival?.delay ?? null;
  const dDelay = stu.departure?.delay ?? null;

  // Prefer absolute time if present
  let ts = Number(aTime || dTime || 0);
  let usedDelayAsTime = false;

  if (!ts && TREAT_DELAY_AS_EPOCH) {
    const candidates = [aDelay, dDelay].filter(v => v != null);
    const epochLike = candidates.find(v => Number(v) > EPOCH_GUESS_THRESHOLD);
    if (epochLike) { ts = Number(epochLike); usedDelayAsTime = true; }
  }

  const delaySeconds = (aDelay ?? dDelay ?? null);
  return { ts: ts || null, usedDelayAsTime, delaySeconds, aTime, dTime, aDelay, dDelay };
}

function pushArrival(stationsObj, baseToStation, sid, route, now, ts, extra = {}) {
  // O(1) station lookup using base stop_id
  const base = sid?.replace(/[NS]$/, "") || sid;   // "101N" -> "101"
  const stationName = baseToStation.get(base) || "Unknown";

  const s = (stationsObj[stationName] ??= {});
  const r = (s[route] ??= []);
  r.push({
    stop_id: sid,
    route,
    direction: dirFromStopId(sid),
    arrival_epoch: ts,
    in_min: ts == null ? null : Math.max(0, Math.floor((ts - now) / 60)), // floor to be conservative
    ...extra
  });
}

function sortAndTrim(stationsObj, maxPerRoute) {
  for (const routes of Object.values(stationsObj)) {
    for (const [route, arrs] of Object.entries(routes)) {
      arrs.sort((a, b) => (a.arrival_epoch ?? Infinity) - (b.arrival_epoch ?? Infinity));
      routes[route] = arrs.slice(0, maxPerRoute);
    }
  }
}

// Speech helpers
const sayArrival = (t) => {
  if (t.in_min == null) return `${t.route} is approaching`;
  if (t.in_min <= 0)   return `${t.route} is arriving now`;
  if (t.in_min === 1)  return `${t.route} in 1 minute`;
  return `${t.route} in ${t.in_min} minutes`;
};
const dedupeSpeech = (arr) => {
  const seen = new Set();
  return arr.filter(a => {
    const key = `${a.route}|${a.direction}|${a.in_min ?? "approach"}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};
// New helper: collapse consecutive arrivals with same route
function collapseSameRoute(arr) {
  let lastRoute = null;
  return arr.map((item, idx) => {
    const currentRoute = item.route;
    let spoken = sayArrival(item);
    if (idx > 0 && currentRoute === lastRoute) {
      // remove route name at start
      spoken = spoken.replace(/^\S+\s+/, "");
    }
    lastRoute = currentRoute;
    return spoken;
  });
}

export default async function handler(req, res) {
  // CORS
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(204).end();

  try {
    const q = parseQuery(req);
    const { stationsResolved, unknownStations, stopIds, stopIdsSet, baseToStation } =
      resolveStops(q.stationRaw, q.stopIdsParam);

    if (!stopIds.length) {
      return res.status(400).json({
        error: "No stop_ids resolved. Provide station=Van Cortlandt Park-242 St[,High St] and/or stop_ids=A40N,..."
      });
    }

    const now = Math.floor(Date.now() / 1000); // Unix time integer
    const stats = { matchedNoTime: 0, matchedWithTime: 0, usedDelayAsTimeCount: 0, totalEntities: 0, totalTripUpdates: 0, totalStopTimeUpdates: 0 };
    const sampleSet = new Set(stopIds);
    const matchedNoTimeSet = new Set();
    const matchedNoTimeDetails = [];
    const stationsObj = {};

    // Fetch all feeds in parallel
    const results = await Promise.allSettled(FEEDS.map(fetchFeed));
    for (const r of results) {
      const msg = (r.status === "fulfilled" ? r.value : null);
      if (!msg) continue;
      stats.totalEntities += msg.entity.length;

      for (const e of msg.entity) {
        const tu = e.tripUpdate;
        if (!tu) continue;
        stats.totalTripUpdates++;

        const route = (tu.trip?.routeId || "?").toUpperCase();
        // If routes filter present, skip other routes
        if (q.routesFilter && !q.routesFilter.includes(route)) continue;

        for (const stu of tu.stopTimeUpdate || []) {
          stats.totalStopTimeUpdates++;
          const sid = stu.stopId;
          const base = baseOf(sid);
          if (!base || !stopIdsSet.has(base)) continue; // O(1) base-id match

          // NEW: filter by direction before pushing
          const direction = dirFromStopId(sid); // "N" | "S" | null
          if (q.routeDirection !== "BOTH" && direction !== q.routeDirection) continue;

          sampleSet.add(sid);

          const { ts, usedDelayAsTime, delaySeconds, aTime, dTime, aDelay, dDelay } = getTimes(stu);

          if (!ts) {
            stats.matchedNoTime++;
            matchedNoTimeSet.add(sid);

            if (q.showDebug) {
              const looksEpoch = v => (v != null && Number(v) > EPOCH_GUESS_THRESHOLD);
              const detail = {
                stop_id: sid, route,
                interpret: {
                  arrival_time_epoch: aTime, arrival_time_iso: asIso(aTime),
                  arrival_delay_seconds: aDelay, arrival_delay_looks_like_epoch: looksEpoch(aDelay),
                  arrival_delay_as_time_iso_if_epoch: looksEpoch(aDelay) ? asIso(aDelay) : null,
                  departure_time_epoch: dTime, departure_time_iso: asIso(dTime),
                  departure_delay_seconds: dDelay, departure_delay_looks_like_epoch: looksEpoch(dDelay),
                  departure_delay_as_time_iso_if_epoch: looksEpoch(dDelay) ? asIso(dDelay) : null
                },
                arrival_raw: q.rawDump ? (stu.arrival || null) : undefined,
                departure_raw: q.rawDump ? (stu.departure || null) : undefined
              };
              matchedNoTimeDetails.push(detail);
            }

            pushArrival(stationsObj, baseToStation, sid, route, now, null, {
              status: "approaching",
              delay_seconds: delaySeconds ?? undefined
            });
            continue;
          }

          // Time window filter
          if ((q.lookbackSeconds === 0 && ts < now) || (q.lookbackSeconds > 0 && ts < now - q.lookbackSeconds)) continue;

          stats.matchedWithTime++;
          if (usedDelayAsTime) stats.usedDelayAsTimeCount++;

          pushArrival(stationsObj, baseToStation, sid, route, now, ts, {
            used_delay_as_time: usedDelayAsTime || undefined
          });
        }
      }
    }

    sortAndTrim(stationsObj, q.maxPerRoute);

    const meta = {
      stations: stationsResolved.length ? stationsResolved : null,
      unknown_stations: unknownStations.length ? unknownStations : null,
      stop_ids: stopIds,
      routes: q.routesFilter || null,
      route_direction: q.routeDirection,
      generated_at: now,
      max_per_route: q.maxPerRoute,
      lookback_seconds: q.lookbackSeconds
    };

    // ---- Speech mode ----
    if (q.format === "speech") {
      const perStationSentences = {};
      for (const [stationName, routes] of Object.entries(stationsObj)) {
        // Flatten and keep route id on each entry
        const all = Object.entries(routes).flatMap(([routeId, arrs]) => arrs.map(a => ({ ...a, route: routeId })));

        // Build per-direction lists, dedup and sort by ETA (approaching at end due to Infinity)
        const byDir = d => dedupeSpeech(
          all.filter(a => a.direction === d)
        ).sort((x, y) => (x.arrival_epoch ?? Infinity) - (y.arrival_epoch ?? Infinity));

        const north = byDir("N").slice(0, q.speechLimit);
        const south = byDir("S").slice(0, q.speechLimit);

        let sentence = `${stationName}: `;
        if (q.routeDirection === "N") {
          sentence += north.length
            ? `(northbound) ${collapseSameRoute(north).join("; ")}.`
            : "(northbound) none.";
        } else if (q.routeDirection === "S") {
          sentence += south.length
            ? `(southbound) ${collapseSameRoute(north).join("; ")}.`
            : "(southbound) none.";
        } else {
          const nPart = north.length
            ? `(northbound) ${collapseSameRoute(north).join("; ")}`
            : "(northbound) none";
          const sPart = south.length
            ? `(southbound) ${collapseSameRoute(north).join("; ")}`
            : "(southbound) none";
          sentence += `${nPart}. ${sPart}.`;
        }

        perStationSentences[stationName] = sentence;
      }

      const ordered = (stationsResolved.length ? stationsResolved : Object.keys(perStationSentences));
      const speech = ordered.map(n => perStationSentences[n]).filter(Boolean).join(" ");

      const speechPayload = {
        meta,
        speech,
        stations_speech: perStationSentences,
        ...(q.showDebug && {
          debug: {
            matched_no_time: stats.matchedNoTime,
            matched_with_time: stats.matchedWithTime,
            used_delay_as_time_count: stats.usedDelayAsTimeCount,
            total_entities: stats.totalEntities,
            total_trip_updates: stats.totalTripUpdates,
            total_stop_time_updates: stats.totalStopTimeUpdates
          }
        })
      };
      res.setHeader("Cache-Control", "s-maxage=15, stale-while-revalidate=30");
      return res.status(200).json(speechPayload);
    }
    // ---- End speech mode ----

    const payload = {
      meta,
      stations: stationsObj,
      ...(q.showDebug && {
        debug: {
          matched_no_time: stats.matchedNoTime,
          matched_with_time: stats.matchedWithTime,
          used_delay_as_time_count: stats.usedDelayAsTimeCount,
          total_entities: stats.totalEntities,
          total_trip_updates: stats.totalTripUpdates,
          total_stop_time_updates: stats.totalStopTimeUpdates,
          sample_stop_ids: Array.from(sampleSet),
          matched_no_time_ids: Array.from(matchedNoTimeSet),
          matched_no_time_details: matchedNoTimeDetails
        }
      })
    };
    res.setHeader("Cache-Control", "s-maxage=15, stale-while-revalidate=30");
    return res.status(200).json(payload);

  } catch (err) {
    return res.status(500).json({ error: err?.message || "server error" });
  }
}
