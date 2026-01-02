// docs/app.js

let DATA = {
  items: [],
  kpi: [],
  series: [],
  aggDim: { almacen: [], servicio: [], medico: [] }
};

let ui = {
  modal: null,
  choicesItems: null,
  choicesAlm: null,
  choicesSrv: null
};

function $(id){ return document.getElementById(id); }

function fmtNum(x, dec=0){
  const v = Number.isFinite(x) ? x : 0;
  return v.toLocaleString("es-ES", { minimumFractionDigits: dec, maximumFractionDigits: dec });
}

function parseISODate(s){
  const d = new Date(s);
  return Number.isNaN(d.getTime()) ? null : d;
}

function toISODate(d){
  const z = new Date(d.getTime() - d.getTimezoneOffset()*60000);
  return z.toISOString().slice(0,10);
}

function getStockMap(){
  try{
    const raw = localStorage.getItem(CONFIG.AUTH.STOCK_KEY);
    if(!raw) return {};
    const obj = JSON.parse(raw);
    return (obj && typeof obj === "object") ? obj : {};
  }catch(_){
    return {};
  }
}

function setStockMap(obj){
  localStorage.setItem(CONFIG.AUTH.STOCK_KEY, JSON.stringify(obj));
}

function clearStock(){
  localStorage.removeItem(CONFIG.AUTH.STOCK_KEY);
}

async function fetchJSON(name){
  const url = `${CONFIG.DATA_DIR}/${name}`;
  const r = await fetch(url, { cache: "no-store" });
  if(!r.ok) throw new Error(`No se pudo cargar ${url}`);
  return await r.json();
}



try{
  await loadData();
}catch(err){
  const box = document.getElementById("dataErrBox");
  box.classList.remove("d-none");
  box.textContent = "Error cargando datos (JSON). Verifique que está en GitHub Pages y que existen /data/items.json, /data/kpi.json, /data/series_day.json, /data/agg_dim.json. Detalle: " + String(err);
  return;
}


function initChoices(){
  ui.choicesItems = new Choices($("#selItems"), { removeItemButton: true, shouldSort: true, placeholder: true });
  ui.choicesAlm = new Choices($("#selAlmacen"), { removeItemButton: true, shouldSort: true, placeholder: true });
  ui.choicesSrv = new Choices($("#selServicio"), { removeItemButton: true, shouldSort: true, placeholder: true });
}

function setSelectOptions(choices, values, selectAll=false){
  choices.clearChoices();
  const opts = values.map(v => ({ value: v, label: v, selected: selectAll }));
  choices.setChoices(opts, "value", "label", true);
}

function getChoiceValues(choices){
  return choices.getValue(true);
}

function uniqNonEmpty(arr){
  const s = new Set();
  arr.forEach(v => {
    const x = (v || "").trim();
    if(x) s.add(x);
  });
  return Array.from(s).sort((a,b)=>a.localeCompare(b,"es"));
}

function inferDateRange(){
  const ds = DATA.series.map(r => r.PERIODO);
  const dates = ds.map(parseISODate).filter(x => x !== null).sort((a,b)=>a-b);
  if(dates.length === 0) return null;
  return { min: dates[0], max: dates[dates.length-1] };
}

function applyDefaultDateRange(){
  const r = inferDateRange();
  if(!r) return;
  $("#dateStart").value = toISODate(r.min);
  $("#dateEnd").value = toISODate(r.max);
}

function kpiLastDate(){
  const r = inferDateRange();
  return r ? toISODate(r.max) : "";
}

function filterSeriesByDate(series, start, end){
  const s = parseISODate(start);
  const e = parseISODate(end);
  if(!s || !e) return series;
  const sT = s.getTime();
  const eT = e.getTime();
  return series.filter(r => {
    const d = parseISODate(r.PERIODO);
    if(!d) return false;
    const t = d.getTime();
    return (t >= sT && t <= eT);
  });
}

function sumQ(rows){
  return rows.reduce((acc,r)=>acc + (Number(r.Q)||0), 0);
}

function buildSeriesChart(seriesRows, selectedItems){
  const byItem = new Map();
  selectedItems.forEach(it => byItem.set(it, []));
  seriesRows.forEach(r => {
    const it = r.ITEM_CRITICO;
    if(!byItem.has(it)) return;
    byItem.get(it).push(r);
  });

  const traces = [];
  for(const [it, rows] of byItem.entries()){
    const xs = rows.map(r => r.PERIODO);
    const ys = rows.map(r => Number(r.Q)||0);
    traces.push({ type:"scatter", mode:"lines+markers", name: it, x: xs, y: ys });
  }

  const layout = {
    margin: { l: 60, r: 20, t: 10, b: 50 },
    xaxis: { title: "Fecha" },
    yaxis: { title: "Cantidad" },
    legend: { orientation: "h" }
  };

  Plotly.newPlot("chartSeries", traces, layout, { responsive: true, displaylogo: false });
}

function buildBarChart(containerId, rows, dimLabel){
  const x = rows.map(r => r.key);
  const y = rows.map(r => r.q);

  const trace = { type:"bar", x, y };
  const layout = {
    margin: { l: 60, r: 20, t: 10, b: 120 },
    xaxis: { title: dimLabel, tickangle: -45 },
    yaxis: { title: "Cantidad" }
  };

  Plotly.newPlot(containerId, [trace], layout, { responsive: true, displaylogo: false });
}

function computeCoverageRows(kpiRows){
  const stock = getStockMap();

  const out = kpiRows.map(r => {
    const item = r.ITEM_CRITICO;
    const total = Number(r.Q_TOTAL)||0;
    const q7 = Number(r.Q_7D)||0;
    const q30 = Number(r.Q_30D)||0;
    const avg = Number(r.AVG_DAILY_30D)||0;

    const st = Number(stock[item] ?? 0);
    const days = (avg > 0) ? (st / avg) : (st > 0 ? Infinity : 0);

    let state = "OK";
    if(!Number.isFinite(days)){
      state = "OK";
    }else if(days < CONFIG.THRESHOLDS.CRIT_DAYS){
      state = "CRITICO";
    }else if(days < CONFIG.THRESHOLDS.WARN_DAYS){
      state = "ALERTA";
    }else{
      state = "OK";
    }

    return {
      item,
      total, q7, q30, avg,
      stock: st,
      days,
      state
    };
  });

  return out;
}

function renderTable(rows){
  const tb = $("#tblMain").querySelector("tbody");
  tb.innerHTML = "";

  const stockMap = getStockMap();

  rows.forEach(r => {
    const tr = document.createElement("tr");

    const tdItem = document.createElement("td");
    tdItem.textContent = r.item;

    const tdTotal = document.createElement("td");
    tdTotal.className = "text-end";
    tdTotal.textContent = fmtNum(r.total);

    const td7 = document.createElement("td");
    td7.className = "text-end";
    td7.textContent = fmtNum(r.q7);

    const td30 = document.createElement("td");
    td30.className = "text-end";
    td30.textContent = fmtNum(r.q30);

    const tdAvg = document.createElement("td");
    tdAvg.className = "text-end";
    tdAvg.textContent = fmtNum(r.avg, 2);

    const tdStock = document.createElement("td");
    tdStock.className = "text-end";
    const inp = document.createElement("input");
    inp.type = "number";
    inp.step = "1";
    inp.min = "0";
    inp.className = "form-control form-control-sm text-end";
    inp.value = String(stockMap[r.item] ?? 0);
    inp.addEventListener("change", () => {
      const v = Number(inp.value || 0);
      const m = getStockMap();
      m[r.item] = Number.isFinite(v) ? v : 0;
      setStockMap(m);
      renderAll();
    });
    tdStock.appendChild(inp);

    const tdDays = document.createElement("td");
    tdDays.className = "text-end";
    tdDays.textContent = Number.isFinite(r.days) ? fmtNum(r.days, 1) : "∞";

    const tdState = document.createElement("td");
    const badge = document.createElement("span");
    badge.className = "badge badge-state";
    if(r.state === "CRITICO"){
      badge.classList.add("state-crit");
      badge.textContent = "CRÍTICO";
    }else if(r.state === "ALERTA"){
      badge.classList.add("state-warn");
      badge.textContent = "ALERTA";
    }else{
      badge.classList.add("state-ok");
      badge.textContent = "OK";
    }
    tdState.appendChild(badge);

    tr.appendChild(tdItem);
    tr.appendChild(tdTotal);
    tr.appendChild(td7);
    tr.appendChild(td30);
    tr.appendChild(tdAvg);
    tr.appendChild(tdStock);
    tr.appendChild(tdDays);
    tr.appendChild(tdState);

    tb.appendChild(tr);
  });
}

function exportCSV(rows){
  const header = ["ITEM","TOTAL","Q_7D","Q_30D","AVG_DAILY_30D","STOCK","DIAS_COBERTURA","ESTADO"];
  const lines = [header.join(",")];

  rows.forEach(r => {
    const vals = [
      `"${String(r.item).replaceAll('"','""')}"`,
      r.total, r.q7, r.q30, r.avg,
      r.stock,
      Number.isFinite(r.days) ? r.days : "",
      r.state
    ];
    lines.push(vals.join(","));
  });

  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);

  const a = document.createElement("a");
  a.href = url;
  a.download = "stock_critico_resumen.csv";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function currentFilters(){
  return {
    start: $("#dateStart").value,
    end: $("#dateEnd").value,
    items: getChoiceValues(ui.choicesItems),
    almacen: getChoiceValues(ui.choicesAlm),
    servicio: getChoiceValues(ui.choicesSrv)
  };
}

function filterAggDim(dimArr, selectedItems){
  const s = new Set(selectedItems);
  const byKey = new Map();

  dimArr.forEach(r => {
    if(!s.has(r.ITEM_CRITICO)) return;
    const k = (r[Object.keys(r).includes("ALMACEN") ? "ALMACEN" : Object.keys(r).includes("SERVICIO") ? "SERVICIO" : "MEDICO"] ?? "").trim();
    const q = Number(r.Q)||0;
    if(!k) return;
    byKey.set(k, (byKey.get(k) || 0) + q);
  });

  const out = Array.from(byKey.entries()).map(([key,q]) => ({ key, q }));
  out.sort((a,b)=>b.q-a.q);
  return out.slice(0, 20);
}

function buildDimensionDropdowns(){
  const alm = uniqNonEmpty(DATA.aggDim.almacen.map(r => r.ALMACEN));
  const srv = uniqNonEmpty(DATA.aggDim.servicio.map(r => r.SERVICIO));
  setSelectOptions(ui.choicesAlm, alm, false);
  setSelectOptions(ui.choicesSrv, srv, false);
}

function applyFiltersAndSummaries(){
  const f = currentFilters();

  const selectedItems = (f.items && f.items.length) ? f.items : DATA.items.map(x => x.ITEM_CRITICO);
  const seriesF = filterSeriesByDate(DATA.series, f.start, f.end).filter(r => selectedItems.includes(r.ITEM_CRITICO));

  const totalQ = sumQ(seriesF);
  $("#kpiTotal").textContent = fmtNum(totalQ);
  $("#kpiLastDate").textContent = kpiLastDate();

  buildSeriesChart(seriesF, selectedItems);

  // Dimensiones globales (no por fecha, porque agg_dim ya viene consolidado)
  const almRows = filterAggDim(DATA.aggDim.almacen, selectedItems);
  const srvRows = filterAggDim(DATA.aggDim.servicio, selectedItems);
  buildBarChart("chartAlmacen", almRows, "Almacén");
  buildBarChart("chartServicio", srvRows, "Servicio");

  // Tabla por item: se basa en KPI total, pero restringida a items seleccionados
  const kpiSel = DATA.kpi.filter(r => selectedItems.includes(r.ITEM_CRITICO));
  const covRows = computeCoverageRows(kpiSel);

  const nCrit = covRows.filter(r => r.state === "CRITICO").length;
  const nWarn = covRows.filter(r => r.state === "ALERTA").length;
  $("#kpiCrit").textContent = fmtNum(nCrit);
  $("#kpiWarn").textContent = fmtNum(nWarn);

  // Orden: crítico, alerta, ok
  const ord = { "CRITICO": 0, "ALERTA": 1, "OK": 2 };
  covRows.sort((a,b) => (ord[a.state]-ord[b.state]) || (b.days-a.days));

  renderTable(covRows);

  return covRows;
}

function renderAll(){
  return applyFiltersAndSummaries();
}

function resetUI(){
  applyDefaultDateRange();

  const allItems = DATA.items.map(x => x.ITEM_CRITICO);
  setSelectOptions(ui.choicesItems, allItems, CONFIG.DEFAULTS.SELECT_ALL_ITEMS);

  ui.choicesAlm.removeActiveItems();
  ui.choicesSrv.removeActiveItems();
}

async function init(){
  // Auth gate
  ui.modal = new bootstrap.Modal($("#loginModal"));

  if(!isAuthed()){
    ui.modal.show();
  }else{
    $("#btnLogout").classList.remove("d-none");
    const u = sessionStorage.getItem(CONFIG.AUTH.USER_BADGE_KEY) || "";
    $("#authUserBadge").textContent = u;
    $("#authUserBadge").classList.remove("d-none");
  }

  $("#btnLogin").addEventListener("click", async () => {
    const ok = await login($("#loginUser").value, $("#loginEmail").value, $("#loginPass").value);
    if(ok){
      $("#loginErr").classList.add("d-none");
      ui.modal.hide();
      $("#btnLogout").classList.remove("d-none");
      const u = sessionStorage.getItem(CONFIG.AUTH.USER_BADGE_KEY) || "";
      $("#authUserBadge").textContent = u;
      $("#authUserBadge").classList.remove("d-none");
    }else{
      $("#loginErr").classList.remove("d-none");
    }
  });

  $("#btnLogout").addEventListener("click", () => {
    clearAuth();
    location.reload();
  });

  $("#btnApply").addEventListener("click", () => {
    renderAll();
  });

  $("#btnReset").addEventListener("click", () => {
    resetUI();
    renderAll();
  });

  $("#btnClearStock").addEventListener("click", () => {
    clearStock();
    renderAll();
  });

  $("#btnExport").addEventListener("click", () => {
    const rows = applyFiltersAndSummaries();
    exportCSV(rows);
  });

  await loadData();
  initChoices();

  buildDimensionDropdowns();

  applyDefaultDateRange();
  const allItems = DATA.items.map(x => x.ITEM_CRITICO);
  setSelectOptions(ui.choicesItems, allItems, CONFIG.DEFAULTS.SELECT_ALL_ITEMS);

  renderAll();
}

document.addEventListener("DOMContentLoaded", () => {
  init();
});

