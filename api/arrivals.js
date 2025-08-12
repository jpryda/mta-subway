// Vercel Serverless Function: MTA GTFS-RT → JSON (no API key)
// - Station resolution from bundled name→baseId map (no hardcoded station map).
// - Base IDs (no N/S) used to match both directions via prefix.
// - format=speech supports speech_limit (default 2) and speech_direction=N|S|BOTH.
// - Epoch-in-delay fallback is ALWAYS ON (treatDelayAsEpoch = true).
// - Debug: show_debug=1 (with optional raw_dump=1) returns interpreter details.

import protobuf from "protobufjs";
import NAME_TO_BASE from "./stops.name_to_id.min.json" assert { type: "json" };

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

// --- Name map indexes (built once per cold start) ---
const norm = s => (s || "").toLowerCase().replace(/street\b/g, "st").replace(/\s+/g, " ").trim();
const NAME_NORM_TO_BASE = {};
const BASE_TO_NAME = {};
for (const [stationName, baseIds] of Object.entries(NAME_TO_BASE)) {
  NAME_NORM_TO_BASE[norm(stationName)] = baseIds;
  for (const baseId of baseIds) BASE_TO_NAME[baseId] = stationName; // canonical GTFS name
}

// Utils
const dirFromStopId = sid => (sid && /[NS]$/.test(sid) ? sid.slice(-1) : null);
const asIso = sec => (sec == null ? null : new Date(Number(sec) * 1000).toISOString());
const prefixMatch = (sid, ids) => !!sid && ids.some(id => sid.startsWith(id));

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

  const stationRaw = qp("station");              // comma-separated station names
  const stopIdsParam = qp("stop_ids");           // optional: user-supplied ids (N/S allowed)
  const maxPerRoute = Math.max(1, Math.min(10, num("max_per_route", 5)));
  const windowSeconds = num("window_seconds", 0); // 0=future-only
  const showDebug = bool("show_debug");
  const rawDump = bool("raw_dump");
  const format = qp("format");                   // "speech"
  const speechLimit = Math.max(1, num("speech_limit", 2));
  const speechDirection = (qp("speech_direction") || "N").toUpperCase(); // N|S|BOTH

  return { stationRaw, stopIdsParam, maxPerRoute, windowSeconds, showDebug, rawDump, format, speechLimit, speechDirection };
}

// Resolve stations from bundled map; produce BASE ids only (no N/S)
function resolveStops(stationRaw, stopIdsParam) {
  const stationsResolved = [];       // canonical station names (for display)
  const unknownStations = [];
  const stopIdsSet = new Set();      // holds BASE ids (e.g., "231")
  const baseToStation = new Map();   // baseId -> canonical name
  const expectedRoutesByStation = {}; // left empty unless you add hints

  if (stationRaw) {
    for (const raw of stationRaw.split(",").map(s => s.trim()).filter(Boolean)) {
      const key = norm(raw);
      const baseIds = NAME_NORM_TO_BASE[key];
      if (baseIds?.length) {
        const canonical = BASE_TO_NAME[baseIds[0]] || raw;
        stationsResolved.push(canonical);
        for (const baseId of baseIds) {
          stopIdsSet.add(baseId);
          baseToStation.set(baseId, canonical);
        }
      } else {
        unknownStations.push(raw);
      }
    }
  }

  if (stopIdsParam) {
    for (const id of stopIdsParam.split(",").map(s => s.trim()).filter(Boolean)) {
      const baseId = id.replace(/[NS]$/, "");
      stopIdsSet.add(baseId);
      if (BASE_TO_NAME[baseId]) baseToStation.set(baseId, BASE_TO_NAME[baseId]);
    }
  }

  return {
    stationsResolved,
    unknownStations,
    stopIds: Array.from(stopIdsSet),
    baseToStation,
    expectedRoutesByStation
  };
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
  let stationName = "Unknown";
  for (const [base, name] of baseToStation.entries()) { if (sid.startsWith(base)) { stationName = name; break; } }
  const s = (stationsObj[stationName] ??= {});
  const r = (s[route] ??= []);
  r.push({
    stop_id: sid,
    route,
    direction: dirFromStopId(sid),
    arrival_epoch: ts,
    in_min: ts == null ? null : Math.max(0, Math.round((ts - now) / 60)),
    ...extra
  });
}

function seedExpectedRoutes(stationsObj, expectedRoutesByStation) {
  for (const [stationName, routes] of Object.entries(expectedRoutesByStation)) {
    const s = (stationsObj[stationName] ??= {});
    for (const rt of routes) if (!s[rt]) s[rt] = [];
  }
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

export default async function handler(req, res) {
  // CORS
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(204).end();

  try {
    const q = parseQuery(req);
    const { stationsResolved, unknownStations, stopIds, baseToStation, expectedRoutesByStation } =
      resolveStops(q.stationRaw, q.stopIdsParam);

    if (!stopIds.length) {
      return res.status(400).json({ error: "No stop_ids resolved. Provide station=Clark St[,High St] and/or stop_ids=A40N,..." });
    }

    const now = Math.floor(Date.now() / 1000);
    const stats = { matchedNoTime: 0, matchedWithTime: 0, usedDelayAsTimeCount: 0, totalEntities: 0, totalTripUpdates: 0, totalStopTimeUpdates: 0 };
    const sampleSet = new Set(stopIds);
    const matchedNoTimeSet = new Set();
    const matchedNoTimeDetails = [];
    const stationsObj = {};
    seedExpectedRoutes(stationsObj, expectedRoutesByStation); // no-op unless you add hints

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

        const route = tu.trip?.routeId || "?";
        for (const stu of tu.stopTimeUpdate || []) {
          stats.totalStopTimeUpdates++;
          const sid = stu.stopId;
          if (!prefixMatch(sid, stopIds)) continue;
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
          if ((q.windowSeconds === 0 && ts < now) || (q.windowSeconds > 0 && ts < now - q.windowSeconds)) continue;

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
      generated_at: now,
      max_per_route: q.maxPerRoute,
      window_seconds: q.windowSeconds,
      expected_routes: stationsResolved.reduce((acc, name) => { acc[name] = expectedRoutesByStation[name] || null; return acc; }, {})
    };

    // ---- Speech mode ----
    if (q.format === "speech") {
      const perStationSentences = {};
      for (const [stationName, routes] of Object.entries(stationsObj)) {
        // Flatten and keep route id on each entry
        const all = Object.entries(routes).flatMap(([routeId, arrs]) => arrs.map(a => ({ ...a, route: routeId })));

        // Build per-direction lists, dedupe and sort by ETA (approaching at end due to Infinity)
        const byDir = d => dedupeSpeech(
          all.filter(a => a.direction === d)
        ).sort((x, y) => (x.arrival_epoch ?? Infinity) - (y.arrival_epoch ?? Infinity));

        const north = byDir("N").slice(0, q.speechLimit);
        const south = byDir("S").slice(0, q.speechLimit);

        let sentence = `${stationName}: `;
        if (q.speechDirection === "N") {
          sentence += north.length
            ? `(northbound) ${north.map(sayArrival).join("; ")}.`
            : "(northbound) none.";
        } else if (q.speechDirection === "S") {
          sentence += south.length
            ? `(southbound) ${south.map(sayArrival).join("; ")}.`
            : "(southbound) none.";
        } else {
          const nPart = north.length
            ? `(northbound) ${north.map(sayArrival).join("; ")}`
            : "(northbound) none";
          const sPart = south.length
            ? `(southbound) ${south.map(sayArrival).join("; ")}`
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
          sample_stop_ids: Array.from(new Set([...sampleSet])),
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
