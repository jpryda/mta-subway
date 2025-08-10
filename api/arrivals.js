// Vercel Serverless Function: MTA GTFS-RT → JSON (no API key)
// - protobuf.parse (no file I/O)
// - camelCase field access for protobufjs
// - prefix match on stop_ids
// - time filtering via ?window_seconds= (0=future-only, >0=allow past window, -1=disable)
// - debug via ?show_debug=1

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

// Minimal GTFS-RT schema (proto2) parsed in-memory
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

// Helpers
const norm = s => (s || "").toLowerCase().replace(/street\b/g, "st").replace(/\s+/g, " ").trim();
const prefixMatch = (sid, ids) => !!sid && ids.some(id => sid.startsWith(id));

// Pre-mapped favorites (extend later)
const STATION_TO_STOP_IDS = {
  // Clark St (2/3)
  "clark st": ["R23N", "R23S", "R33N", "R33S"],
  "clark street": ["R23N", "R23S", "R33N", "R33S"],
  // High St (A/C)
  "high st": ["A41N", "A41S", "C41N", "C41S"],
  "high street": ["A41N", "A41S", "C41N", "C41S"]
};

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
  } catch {
    return null; // transient errors / non-protobuf responses
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
    const stationParam = url.searchParams.get("station");
    const stopIdsParam = url.searchParams.get("stop_ids");
    const maxPerRoute = Math.max(1, Math.min(10, Number(url.searchParams.get("max_per_route") || 5)));

    // Time/debug controls
    const showDebug = url.searchParams.get("show_debug") === "1";
    const windowRaw = url.searchParams.get("window_seconds");
    const windowSeconds = windowRaw === "-1" ? -1 : Number(windowRaw || 0); // 0=future-only, >0=past window, -1=off

    // Build stopIds
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
    const results = [];
    let matchedNoTime = 0;
    let m

