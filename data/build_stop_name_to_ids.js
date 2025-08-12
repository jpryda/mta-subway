// build_stop_name_to_ids.js
// Input:  stops.txt (GTFS stops file)
// Output: stops.name_to_id.json -> { "Van Cortlandt Park-242 St": ["101"], ... }

import fs from "fs";
const csv = fs.readFileSync("stops.txt", "utf8").replace(/^\uFEFF/, "").trim().split(/\r?\n/);
const [hdr, ...rows] = csv;
const H = hdr.split(",");
const idx = k => H.indexOf(k);
const splitCSV = l => l.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/).map(s => s.replace(/^"|"$/g, "").replace(/""/g, '"'));
const iStopId = idx("stop_id"), iStopName = idx("stop_name"), iParent = idx("parent_station"), iLocType = idx("location_type");
if (iStopId < 0 || iStopName < 0 || iParent < 0 || iLocType < 0) throw new Error("Missing required columns");

const base = {};
for (const line of rows) {
  if (!line.trim()) continue;
  const c = splitCSV(line);
  const stop_id = c[iStopId], stop_name = c[iStopName], parent = c[iParent] || "", loc = c[iLocType] || "";
  const baseId = parent || stop_id;
  const rec = (base[baseId] ||= { name: null, hasN: false, hasS: false });
  if (loc === "1" || rec.name == null) rec.name = stop_name;
  if (parent) {
    if (stop_id.endsWith("N")) rec.hasN = true;
    if (stop_id.endsWith("S")) rec.hasS = true;
  }
}

const out = {};
for (const [baseId, info] of Object.entries(base)) {
  if (info.hasN && info.hasS && info.name) (out[info.name] ||= []).push(baseId);
}

for (const k of Object.keys(out)) out[k].sort();

fs.writeFileSync("stops.name_to_id.json", JSON.stringify(out));
console.log("Wrote stops.name_to_id.json");
