const STATE_CENTROIDS = {
  AL: [32.806671, -86.79113], AK: [61.370716, -152.404419], AZ: [33.729759, -111.431221], AR: [34.969704, -92.373123],
  CA: [36.116203, -119.681564], CO: [39.059811, -105.311104], CT: [41.597782, -72.755371], DC: [38.9072, -77.0369],
  DE: [39.318523, -75.507141], FL: [27.766279, -81.686783], GA: [33.040619, -83.643074], HI: [21.094318, -157.498337],
  ID: [44.240459, -114.478828], IL: [40.349457, -88.986137], IN: [39.849426, -86.258278], IA: [42.011539, -93.210526],
  KS: [38.5266, -96.726486], KY: [37.66814, -84.670067], LA: [31.169546, -91.867805], ME: [44.693947, -69.381927],
  MD: [39.063946, -76.802101], MA: [42.230171, -71.530106], MI: [43.326618, -84.536095], MN: [45.694454, -93.900192],
  MS: [32.741646, -89.678696], MO: [38.456085, -92.288368], MT: [46.921925, -110.454353], NE: [41.12537, -98.268082],
  NV: [38.313515, -117.055374], NH: [43.452492, -71.563896], NJ: [40.298904, -74.521011], NM: [34.840515, -106.248482],
  NY: [42.165726, -74.948051], NC: [35.630066, -79.806419], ND: [47.528912, -99.784012], OH: [40.388783, -82.764915],
  OK: [35.565342, -96.928917], OR: [44.572021, -122.070938], PA: [40.590752, -77.209755], RI: [41.680893, -71.51178],
  SC: [33.856892, -80.945007], SD: [44.299782, -99.438828], TN: [35.747845, -86.692345], TX: [31.054487, -97.563461],
  UT: [40.150032, -111.862434], VT: [44.045876, -72.710686], VA: [37.769337, -78.169968], WA: [47.400902, -121.490494],
  WV: [38.491226, -80.954453], WI: [44.268543, -89.616508], WY: [42.755966, -107.30249]
};

const DATASETS = {
  asian: {
    paths: ["processed_data/asian_org_geocoded.csv", "raw_data/asian_org.csv"],
    color: "#0f766e",
    mapId: "asian-map",
    summaryId: "asian-summary",
    statePickId: "asian-state-pick",
    label: "Asian"
  },
  latino: {
    paths: ["processed_data/latino_org_geocoded.csv", "raw_data/latino_org.csv"],
    color: "#c2410c",
    mapId: "latino-map",
    summaryId: "latino-summary",
    statePickId: "latino-state-pick",
    label: "Latino"
  }
};

const appState = {
  rows: { asian: [], latino: [] },
  maps: {},
  selections: { asian: null, latino: null }
};

const typeFilterEl = document.getElementById("type-filter");
const searchFilterEl = document.getElementById("search-filter");
const yearOpEl = document.getElementById("year-op");
const yearValueEl = document.getElementById("year-value");
const recordsBodyEl = document.getElementById("records-body");
const tableSummaryEl = document.getElementById("table-summary");
const tableFilterDatasetEl = document.getElementById("table-filter-dataset");
const tableFilterNameEl = document.getElementById("table-filter-name");
const tableFilterTypeEl = document.getElementById("table-filter-type");
const tableFilterYearOpEl = document.getElementById("table-filter-year-op");
const tableFilterYearEl = document.getElementById("table-filter-year");
const tableFilterCityEl = document.getElementById("table-filter-city");
const tableFilterStateEl = document.getElementById("table-filter-state");
const tableFilterCountyEl = document.getElementById("table-filter-county");

function matchesYear(year, op, rawValue) {
  const value = Number.parseInt(rawValue, 10);
  if (Number.isNaN(value)) return true;
  if (year === null) return false;

  if (op === "eq") return year === value;
  if (op === "gt") return year > value;
  if (op === "lt") return year < value;
  if (op === "lte") return year <= value;
  return year >= value;
}

async function parseCsvText(csvText) {
  return new Promise((resolve, reject) => {
    Papa.parse(csvText, {
      header: true,
      skipEmptyLines: true,
      complete: (result) => resolve(result.data),
      error: reject
    });
  });
}

async function loadCsvWithFallback(paths) {
  for (const path of paths) {
    try {
      const response = await fetch(path);
      if (!response.ok) continue;
      const text = await response.text();
      const data = await parseCsvText(text);
      if (data.length > 0) return data;
    } catch (_err) {
      // fallback to next path
    }
  }
  throw new Error(`Could not load any CSV: ${paths.join(", ")}`);
}

function normalizeRow(row, datasetKey) {
  const year = Number.parseInt((row["F.year"] || "").trim(), 10);
  const lat = Number.parseFloat((row.latitude || "").trim());
  const lon = Number.parseFloat((row.longitude || "").trim());

  return {
    datasetKey,
    datasetLabel: DATASETS[datasetKey].label,
    name: (row.Name || "").trim(),
    foundingYear: Number.isNaN(year) ? null : year,
    address: (row.Address || "").trim(),
    state: (row.States || "").trim().toUpperCase(),
    city: (row.City || "").trim(),
    county: (row.County || "").trim(),
    type: (row.Type || "").trim() || "Unknown",
    latitude: Number.isFinite(lat) ? lat : null,
    longitude: Number.isFinite(lon) ? lon : null
  };
}

function initMap(containerId) {
  const map = L.map(containerId, { zoomControl: true, minZoom: 3 }).setView([39.5, -98.35], 4);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
  }).addTo(map);
  return map;
}

function filteredRows(rows) {
  const selectedType = typeFilterEl.value;
  const q = searchFilterEl.value.trim().toLowerCase();
  const yearOp = yearOpEl.value;
  const yearValue = yearValueEl.value;

  return rows.filter((row) => {
    if (selectedType !== "all" && row.type !== selectedType) return false;
    if (!matchesYear(row.foundingYear, yearOp, yearValue)) return false;
    if (q && !row.name.toLowerCase().includes(q)) return false;
    return true;
  });
}

function displayType(type) {
  return type === "CSO" ? "Community Service Organization" : type;
}

function popupHtml(row) {
  return [
    `<strong>${escapeHtml(row.name)}</strong>`,
    `${escapeHtml(displayType(row.type))} | ${row.foundingYear ?? "Year n/a"}`,
    `${escapeHtml(row.city)}${row.city && row.state ? ", " : ""}${escapeHtml(row.state)}`,
    row.address ? escapeHtml(row.address) : ""
  ]
    .filter(Boolean)
    .join("<br>");
}

function drawDatasetMap(datasetKey) {
  const cfg = DATASETS[datasetKey];
  const rows = filteredRows(appState.rows[datasetKey]);

  const mapEntry = appState.maps[datasetKey];
  if (!mapEntry || !mapEntry.map) return;
  if (!mapEntry.layer) {
    mapEntry.layer = L.layerGroup().addTo(mapEntry.map);
  }
  mapEntry.layer.clearLayers();

  const pointRows = rows.filter((r) => r.latitude !== null && r.longitude !== null);
  const fallbackRows = rows.filter((r) => r.latitude === null || r.longitude === null);

  for (const row of pointRows) {
    const marker = L.circleMarker([row.latitude, row.longitude], {
      radius: 4,
      color: cfg.color,
      weight: 1,
      fillColor: cfg.color,
      fillOpacity: 0.5
    });
    marker.bindPopup(popupHtml(row));
    marker.on("click", () => {
      appState.selections[datasetKey] = row.state || null;
      updateStatePill(datasetKey);
      renderTable();
    });
    marker.addTo(mapEntry.layer);
  }

  const byStateFallback = new Map();
  for (const row of fallbackRows) {
    if (!row.state) continue;
    if (!byStateFallback.has(row.state)) byStateFallback.set(row.state, []);
    byStateFallback.get(row.state).push(row);
  }

  const maxFallback = Math.max(1, ...Array.from(byStateFallback.values(), (items) => items.length));
  byStateFallback.forEach((items, state) => {
    const center = STATE_CENTROIDS[state];
    if (!center) return;
    const marker = L.circleMarker(center, {
      radius: 8 + Math.sqrt(items.length / maxFallback) * 16,
      color: "#475569",
      weight: 1,
      fillColor: "#94a3b8",
      fillOpacity: 0.3
    });
    marker.bindTooltip(`${state}: ${items.length} unmatched addresses`);
    marker.on("click", () => {
      appState.selections[datasetKey] = state;
      updateStatePill(datasetKey);
      renderTable();
    });
    marker.addTo(mapEntry.layer);
  });

  const selectedState = appState.selections[datasetKey];
  const availableStates = new Set(rows.map((r) => r.state).filter(Boolean));
  if (selectedState && !availableStates.has(selectedState)) {
    appState.selections[datasetKey] = null;
  }

  document.getElementById(cfg.summaryId).textContent =
    `${rows.length} orgs | ${pointRows.length} point-mapped`;
  updateStatePill(datasetKey);

  if (!mapEntry.legend) {
    mapEntry.legend = L.control({ position: "bottomleft" });
    mapEntry.legend.onAdd = () => {
      const div = L.DomUtil.create("div", "legend");
      div.innerHTML = "Color = organization points; gray = unmatched-by-address totals";
      return div;
    };
    mapEntry.legend.addTo(mapEntry.map);
  }
}

function updateStatePill(datasetKey) {
  const cfg = DATASETS[datasetKey];
  const state = appState.selections[datasetKey];
  const node = document.getElementById(cfg.statePickId);
  node.textContent = state ? `Selected state: ${state} (click text to clear)` : "";
}

function tableRows() {
  let combined = [...filteredRows(appState.rows.asian), ...filteredRows(appState.rows.latino)];

  if (appState.selections.asian) {
    combined = combined.filter((r) => r.datasetKey !== "asian" || r.state === appState.selections.asian);
  }
  if (appState.selections.latino) {
    combined = combined.filter((r) => r.datasetKey !== "latino" || r.state === appState.selections.latino);
  }

  const datasetQ = tableFilterDatasetEl.value.trim();
  const nameQ = tableFilterNameEl.value.trim().toLowerCase();
  const typeQ = tableFilterTypeEl.value.trim();
  const yearOpQ = tableFilterYearOpEl.value;
  const yearQ = tableFilterYearEl.value.trim();
  const cityQ = tableFilterCityEl.value.trim().toLowerCase();
  const stateQ = tableFilterStateEl.value.trim().toUpperCase();
  const countyQ = tableFilterCountyEl.value.trim().toLowerCase();

  if (datasetQ) combined = combined.filter((r) => r.datasetLabel === datasetQ);
  if (nameQ) combined = combined.filter((r) => r.name.toLowerCase().includes(nameQ));
  if (typeQ) combined = combined.filter((r) => r.type === typeQ);
  if (yearQ) combined = combined.filter((r) => matchesYear(r.foundingYear, yearOpQ, yearQ));
  if (cityQ) combined = combined.filter((r) => r.city.toLowerCase().includes(cityQ));
  if (stateQ) combined = combined.filter((r) => r.state.includes(stateQ));
  if (countyQ) combined = combined.filter((r) => r.county.toLowerCase().includes(countyQ));

  combined.sort((a, b) => {
    if (a.datasetKey !== b.datasetKey) return a.datasetKey.localeCompare(b.datasetKey);
    if ((a.foundingYear ?? 99999) !== (b.foundingYear ?? 99999)) return (a.foundingYear ?? 99999) - (b.foundingYear ?? 99999);
    return a.name.localeCompare(b.name);
  });

  return combined;
}

function renderTable() {
  const rows = tableRows();
  recordsBodyEl.innerHTML = rows
    .map(
      (r) => `<tr>
      <td>${r.datasetLabel}</td>
      <td>${escapeHtml(r.name)}</td>
      <td>${escapeHtml(displayType(r.type))}</td>
      <td>${r.foundingYear ?? ""}</td>
      <td>${escapeHtml(r.city)}</td>
      <td>${escapeHtml(r.state)}</td>
      <td>${escapeHtml(r.county)}</td>
    </tr>`
    )
    .join("");

  tableSummaryEl.textContent = `Showing ${rows.length} matching records`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function renderAll() {
  drawDatasetMap("asian");
  drawDatasetMap("latino");
  renderTable();
}

function bindControls() {
  const rerender = () => {
    appState.selections.asian = null;
    appState.selections.latino = null;
    renderAll();
  };

  typeFilterEl.addEventListener("change", rerender);
  searchFilterEl.addEventListener("input", rerender);
  yearOpEl.addEventListener("change", rerender);
  yearValueEl.addEventListener("input", rerender);

  document.getElementById("asian-state-pick").addEventListener("click", () => {
    appState.selections.asian = null;
    renderAll();
  });
  document.getElementById("latino-state-pick").addEventListener("click", () => {
    appState.selections.latino = null;
    renderAll();
  });

  const tableOnlyRerender = () => {
    renderTable();
  };
  tableFilterDatasetEl.addEventListener("change", tableOnlyRerender);
  tableFilterTypeEl.addEventListener("change", tableOnlyRerender);
  tableFilterYearOpEl.addEventListener("change", tableOnlyRerender);
  tableFilterNameEl.addEventListener("input", tableOnlyRerender);
  tableFilterYearEl.addEventListener("input", tableOnlyRerender);
  tableFilterCityEl.addEventListener("input", tableOnlyRerender);
  tableFilterStateEl.addEventListener("input", tableOnlyRerender);
  tableFilterCountyEl.addEventListener("input", tableOnlyRerender);
}

async function init() {
  const [asianRows, latinoRows] = await Promise.all([
    loadCsvWithFallback(DATASETS.asian.paths),
    loadCsvWithFallback(DATASETS.latino.paths)
  ]);

  appState.rows.asian = asianRows.map((row) => normalizeRow(row, "asian"));
  appState.rows.latino = latinoRows.map((row) => normalizeRow(row, "latino"));

  appState.maps.asian = { map: initMap(DATASETS.asian.mapId), layer: null, legend: null };
  appState.maps.latino = { map: initMap(DATASETS.latino.mapId), layer: null, legend: null };

  bindControls();
  renderAll();
}

init().catch((err) => {
  tableSummaryEl.textContent = "Failed to load CSV files.";
  console.error(err);
});
