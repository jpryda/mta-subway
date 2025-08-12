// Vercel Serverless Function: MTA GTFS-RT → JSON (no API key)
// Grouped by station; multi-station; prefix stop_id match; rich debug; route+direction on every entry.
// Fallback for feeds that put epoch in delay; flags used_delay_as_time on entries.
// format=speech supports speech_limit (default 2) and speech_direction=N|S|both.

import protobuf from "protobufjs";

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

// GTFS-RT schema (stop_id = 4; Event reads time/delay/uncertainty)
const SCHEMA = `
  syntax = "proto2";
  package transit_realtime;
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
  message Event {
    optional int64 time = 1;
    optional int32 delay = 2;
    optional int32 uncertainty = 3;
  }
`;

const root = protobuf.parse(SCHEMA).root;
const FeedMessage = root.lookupType("transit_realtime.FeedMessage");

const norm = s => (s || "").toLowerCase().replace(/street\b/g, "st").replace(/\s+/g, " ").trim();
const dirFromStopId = sid => (sid && /[NS]$/.test(sid) ? sid.slice(-1) : null);

// Station → stop_ids
const STATION_TO_STOP_IDS = {
  // Clark St (2/3)
  "clark st": ["231N", "231S"],
  "clark street": ["231N", "231S"],
  // High St (A/C)
  "high st": ["A40N", "A40S"],
  "high street": ["A40N", "A40S"]
};

// Station → expected routes (for JSON display)
const STATION_EXPECTED_ROUTES = {
  "clark st": ["2", "3"],
  "clark street": ["2", "3"],
  "high st": ["A", "C"],
  "high street": ["A", "C"]
};

function prefixMatch(sid, ids) { return !!sid && ids.some(id => sid.startsWith(id)); }

async function fetchFeed(path) {
  try {
    const r = await fetch(API_BASE + path, {
      headers: { "Accept": "application/x-protobuf", "User-Agent": "mta-arrivals/2.2 (+vercel)" }
    });
    if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
    const buf = new Uint8Array(await r.arrayBuffer());
    return FeedMessage.decode(buf);
  } catch {
    return null;
  }
}

// Debug helpers
function asIso(sec) {
  if (sec == null) return null;
  const n = Number(sec);
  if (!Number.isFinite(n)) return null;
  return new Date(n * 1000).toISOString();
}
// Anything > 2005-01-01 looks like an epoch, not a delay
const EPOCH_GUESS_THRESHOLD = 1104537600;

export default async function handler(req, res) {
  // CORS
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") return res.status(204).end();

  try {
    const url = new URL(req.url, "http://localhost");
    const stationRaw = url.searchParams.get("station");
    const stopIdsParam = url.searchParams.get("stop_ids");
    const maxPerRoute = Math.max(1, Math.min(10, Number(url.searchParams.get("max_per_route") || 5)));
    const windowSeconds = Number(url.searchParams.get("window_seconds") || 0); // default: future-only
    const showDebug = url.searchParams.get("show_debug") === "1";
    const rawDump = url.searchParams.get("raw_dump") === "1";
    const format = url.searchParams.get("format"); // "speech" to enable speech mode
    const speechLimit = Math.max(1, Number(url.searchParams.get("speech_limit") || 2));
    const speechDirection = (url.searchParams.get("speech_direction") || "N").toUpperCase(); // N|S|BOTH

    // Resolve inputs
    const stationsResolved = [];
    const unknownStations = [];
    const stopIdsSet = new Set();
    const baseToStation = new Map();
    const expectedRoutesByStation = {};

    if (stationRaw) {
      for (const name of stationRaw.split(",").map(s => s.trim()).filter(Boolean)) {
        const key = norm(name);
        const mappedIds = STATION_TO_STOP_IDS[key];
        if (mappedIds?.length) {
          stationsResolved.push(name);
          for (const id of mappedIds) { stopIdsSet.add(id); baseToStation.set(id, name); }
          expectedRoutesByStation[name] = STATION_EXPECTED_ROUTES[key] || [];
        } else {
          unknownStations.push(name);
        }
      }
    }

    if (stopIdsParam) {
      for (const id of stopIdsParam.split(",").map(s => s.trim()).filter(Boolean)) stopIdsSet.add(id);
    }

    const stopIds = Array.from(stopIdsSet);
    if (!stopIds.length) {
      return res.status(400).json({ error: "No stop_ids resolved. Provide station=Clark St[,High St] and/or stop_ids=A40N,..." });
    }

    const now = Math.floor(Date.now() / 1000);
    let matchedNoTime = 0, matchedWithTime = 0, usedDelayAsTimeCount = 0;
    let totalEntities = 0, totalTripUpdates = 0, totalStopTimeUpdates = 0;

    const sampleSet = new Set();
    const matchedNoTimeSet = new Set();
    const matchedNoTimeDetails = [];

    // station -> route -> [arrivals]
    const stationsObj = {};

    // Seed expected empty routes for JSON
    for (const [stationName, routes] of Object.entries(expectedRoutesByStation)) {
      const s = (stationsObj[stationName] ??= {});
      for (const r of routes) if (!s[r]) s[r] = [];
    }

    function pushArrival(sid, route, ts, entry) {
      let stationName = "Unknown";
      for (const [base, name] of baseToStation.entries()) {
        if (sid.startsWith(base)) { stationName = name; break; }
      }
      const s = (stationsObj[stationName] ??= {});
      const r = (s[route] ??= []);
      const final = entry || {
        stop_id: sid,
        route,
        direction: dirFromStopId(sid),
        arrival_epoch: ts,
        in_min: Math.max(0, Math.round((ts - now) / 60))
      };
      if (!final.route) final.route = route;
      if (!final.direction) final.direction = dirFromStopId(sid);
      r.push(final);
    }

    function maybeAddSample(sid) { if (sid) sampleSet.add(sid); }

    // Pull feeds
    for (const f of FEEDS) {
      const msg = await fetchFeed(f);
      if (!msg) continue;
      totalEntities += msg.entity.length;

      for (const e of msg.entity) {
        const tu = e.tripUpdate;
        if (!tu) continue;
        totalTripUpdates++;

        for (const stu of tu.stopTimeUpdate || []) {
          totalStopTimeUpdates++;

          const sid = stu.stopId;
          maybeAddSample(sid);
          if (!prefixMatch(sid, stopIds)) continue;

          const route = tu.trip?.routeId || "?";
          const aTime = stu.arrival?.time ?? null;
          const dTime = stu.departure?.time ?? null;
          const aDelay = stu.arrival?.delay ?? null;
          const dDelay = stu.departure?.delay ?? null;

          // 1) Prefer absolute time if present
          let ts = Number(aTime || dTime || 0);
          let usedDelayAsTime = false;

          // 2) Fallback: treat epoch-like delay as time
          if (!ts) {
            const delayCandidates = [aDelay, dDelay].filter(v => v != null);
            const delayEpoch = delayCandidates.find(v => Number(v) > EPOCH_GUESS_THRESHOLD);
            if (delayEpoch) {
              ts = Number(delayEpoch);
              usedDelayAsTime = true;
              usedDelayAsTimeCount++;
            }
          }

          if (!ts) {
            // "Approaching" with no ETA
            matchedNoTime++;
            matchedNoTimeSet.add(sid);

            if (showDebug) {
              const detail = {
                stop_id: sid,
                route,
                interpret: {
                  arrival_time_epoch: aTime, arrival_time_iso: asIso(aTime),
                  arrival_delay_seconds: aDelay,
                  arrival_delay_looks_like_epoch: (aDelay != null && Number(aDelay) > EPOCH_GUESS_THRESHOLD),
                  arrival_delay_as_time_iso_if_epoch: (aDelay != null && Number(aDelay) > EPOCH_GUESS_THRESHOLD) ? asIso(aDelay) : null,
                  departure_time_epoch: dTime, departure_time_iso: asIso(dTime),
                  departure_delay_seconds: dDelay,
                  departure_delay_looks_like_epoch: (dDelay != null && Number(dDelay) > EPOCH_GUESS_THRESHOLD),
                  departure_delay_as_time_iso_if_epoch: (dDelay != null && Number(dDelay) > EPOCH_GUESS_THRESHOLD) ? asIso(dDelay) : null
                }
              };
              if (rawDump) {
                detail.arrival_raw = stu.arrival || null;
                detail.departure_raw = stu.departure || null;
              }
              matchedNoTimeDetails.push(detail);
            }

            const entry = {
              stop_id: sid,
              route,
              direction: dirFromStopId(sid),
              arrival_epoch: null,
              in_min: null,
              status: "approaching"
            };
            const delaySec = (aDelay ?? dDelay ?? null);
            if (delaySec != null) entry.delay_seconds = delaySec;
            pushArrival(sid, route, null, entry);
            continue;
          }

          // Time filter
          if (windowSeconds === 0) {
            if (ts < now) continue;
          } else {
            if (ts < now - windowSeconds) continue;
          }

          matchedWithTime++;
          const entry = {
            stop_id: sid,
            route,
            direction: dirFromStopId(sid),
            arrival_epoch: ts,
            in_min: Math.max(0, Math.round((ts - now) / 60))
          };
          if (usedDelayAsTime) entry.used_delay_as_time = true;

          pushArrival(sid, route, ts, entry);
        }
      }
    }

    // Sort & trim JSON arrays
    for (const [stationName, routes] of Object.entries(stationsObj)) {
      for (const [route, arrs] of Object.entries(routes)) {
        arrs.sort((a, b) => (a.arrival_epoch ?? Infinity) - (b.arrival_epoch ?? Infinity));
        routes[route] = arrs.slice(0, maxPerRoute);
      }
    }

    const meta = {
      stations: stationsResolved.length ? stationsResolved : null,
      unknown_stations: unknownStations.length ? unknownStations : null,
      stop_ids: stopIds,
      generated_at: Math.floor(Date.now() / 1000),
      max_per_route: maxPerRoute,
      window_seconds: windowSeconds,
      expected_routes: stationsResolved.reduce((acc, name) => {
        acc[name] = expectedRoutesByStation[name] || null;
        return acc;
      }, {})
    };

    // ---- Speech mode ----
    if (format === "speech") {
      function fmtMinutes(n) {
        if (n == null) return "approaching";
        if (n <= 0) return "now";
        if (n === 1) return "1 minute";
        return `${n} minutes`;
      }
      function dirWord(code) {
        if (code === "N") return "northbound";
        if (code === "S") return "southbound";
        return "";
      }

      const perStationSentences = {};
      for (const [stationName, routes] of Object.entries(stationsObj)) {
        // Collect arrivals across all routes and split by direction
        const all = [];
        for (const [routeId, arrs] of Object.entries(routes)) {
          for (const a of arrs) all.push({ route: routeId, ...a });
        }
        const north = all.filter(a => a.direction === "N").sort((x, y) => (x.arrival_epoch ?? Infinity) - (y.arrival_epoch ?? Infinity));
        const south = all.filter(a => a.direction === "S").sort((x, y) => (x.arrival_epoch ?? Infinity) - (y.arrival_epoch ?? Infinity));

        const speechLimit = Math.max(1, Number(url.searchParams.get("speech_limit") || 2));
        const speechDirection = (url.searchParams.get("speech_direction") || "N").toUpperCase(); // N|S|BOTH

        let sentence = `${stationName}: `;

        if (speechDirection === "N") {
          const top = north.slice(0, speechLimit);
          sentence += top.length
            ? `(northbound) ${top.map(t => `${t.route} in ${fmtMinutes(t.in_min)}`).join("; ")}.`
            : "(northbound) none.";
        } else if (speechDirection === "S") {
          const top = south.slice(0, speechLimit);
          sentence += top.length
            ? `(southbound) ${top.map(t => `${t.route} in ${fmtMinutes(t.in_min)}`).join("; ")}.`
            : "(southbound) none.";
        } else { // BOTH
          const nTop = north.slice(0, speechLimit);
          const sTop = south.slice(0, speechLimit);
          const parts = [];
          parts.push(nTop.length
            ? `(northbound) ${nTop.map(t => `${t.route} in ${fmtMinutes(t.in_min)}`).join("; ")}`
            : "(northbound) none");
          parts.push(sTop.length
            ? `(southbound) ${sTop.map(t => `${t.route} in ${fmtMinutes(t.in_min)}`).join("; ")}`
            : "(southbound) none");
          sentence += parts.join(". ") + ".";
        }

        perStationSentences[stationName] = sentence;
      }

      const ordered = (stationsResolved.length ? stationsResolved : Object.keys(perStationSentences));
      const speech = ordered.map(n => perStationSentences[n]).filter(Boolean).join(" ");

      const meta = { /* keep your existing meta build here */ };
      const speechPayload = { meta, speech, stations_speech: perStationSentences };
      if (showDebug) {
        speechPayload.debug = {
          matched_no_time, matched_with_time, used_delay_as_time_count,
          total_entities, total_trip_updates, total_stop_time_updates
        };
      }
      res.setHeader("Cache-Control", "s-maxage=15, stale-while-revalidate=30");
      return res.status(200).json(speechPayload);
    }
    // ---- End speech mode ----


    // Default JSON (non-speech)
    const payload = { meta, stations: stationsObj };
    if (showDebug) {
      for (const id of stopIds) sampleSet.add(id);
      payload.debug = {
        matched_no_time, matched_with_time, used_delay_as_time_count,
        total_entities, total_trip_updates, total_stop_time_updates,
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
