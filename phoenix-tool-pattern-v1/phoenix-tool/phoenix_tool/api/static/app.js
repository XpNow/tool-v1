const api = (path) => fetch(path).then((r) => r.json());

const state = {
  entity: null,
  view: "search",
};

const viewTitle = document.getElementById("view-title");
const viewHelp = document.getElementById("view-help");
const controls = document.getElementById("controls");
const results = document.getElementById("results");
const recentEntities = document.getElementById("recent-entities");
const globalSearch = document.getElementById("global-search");

const viewConfig = {
  search: { title: "Search", help: "Find events by entity, type, or item." },
  summary: { title: "Summary", help: "Quick overview of totals and top partners." },
  storages: { title: "Storages", help: "Container balances and inventory movement." },
  flow: { title: "Flow", help: "Trace time-coherent chains." },
  trace: { title: "Trace", help: "Network adjacency within a depth." },
  between: { title: "Between", help: "Find events connecting two entities." },
  reports: { title: "Reports", help: "Generate or open reports for an entity." },
};

function setView(view) {
  state.view = view;
  const cfg = viewConfig[view] || viewConfig.search;
  viewTitle.textContent = cfg.title;
  viewHelp.textContent = cfg.help;
  renderControls();
  renderEmpty();
}

function renderControls() {
  controls.innerHTML = "";
  const card = document.getElementById("card-template").content.firstElementChild.cloneNode(true);
  card.classList.add("grid", "gap-3");

  const entityInput = document.createElement("input");
  entityInput.placeholder = "Entity ID";
  entityInput.value = state.entity || "";
  entityInput.className = "rounded-lg bg-slate-900 border border-slate-700 px-3 py-2";
  entityInput.addEventListener("change", (e) => {
    state.entity = e.target.value || null;
  });

  const actionBtn = document.createElement("button");
  actionBtn.textContent = "Run";
  actionBtn.className = "bg-blue-500 hover:bg-blue-600 text-white rounded-lg px-3 py-2";
  actionBtn.addEventListener("click", () => loadView());

  card.appendChild(entityInput);
  card.appendChild(actionBtn);
  controls.appendChild(card);
}

function renderEmpty() {
  results.innerHTML = "";
  const empty = document.getElementById("empty-state-template").content.firstElementChild.cloneNode(true);
  results.appendChild(empty);
}

function renderCard(title, bodyHtml) {
  const card = document.getElementById("card-template").content.firstElementChild.cloneNode(true);
  card.innerHTML = `<h3 class="text-lg font-semibold">${title}</h3><div class="text-sm text-slate-200 mt-2 font-mono">${bodyHtml}</div>`;
  results.appendChild(card);
}

async function loadRecent() {
  const data = await api("/entities?limit=6");
  recentEntities.innerHTML = "";
  data.data.entities.forEach((ent) => {
    const btn = document.createElement("button");
    btn.className = "w-full text-left px-3 py-2 rounded-lg bg-slate-800 border border-slate-700 hover:bg-slate-700";
    btn.textContent = `${ent.name || "UNKNOWN"} [${ent.player_id}]`;
    btn.addEventListener("click", () => {
      state.entity = ent.player_id;
      globalSearch.value = ent.player_id;
      loadView();
    });
    recentEntities.appendChild(btn);
  });
}

async function loadView() {
  results.innerHTML = "";
  if (!state.entity && state.view !== "between") {
    renderEmpty();
    return;
  }

  if (state.view === "summary") {
    const data = await api(`/summary?entity=${state.entity}`);
    renderCard("Summary", JSON.stringify(data.data, null, 2));
    return;
  }

  if (state.view === "storages") {
    const data = await api(`/storages?entity=${state.entity}`);
    renderCard("Storages", JSON.stringify(data.data, null, 2));
    return;
  }

  if (state.view === "flow") {
    const data = await api(`/flow?entity=${state.entity}`);
    renderCard("Flow", JSON.stringify(data.data, null, 2));
    return;
  }

  if (state.view === "trace") {
    const data = await api(`/trace?entity=${state.entity}`);
    renderCard("Trace", JSON.stringify(data.data, null, 2));
    return;
  }

  if (state.view === "between") {
    renderCard("Between", "Enter two entities in the global search separated by comma.");
    return;
  }

  if (state.view === "reports") {
    renderCard("Reports", "Use CLI: report <id> to generate report packs.");
    return;
  }

  const data = await api(`/events?entity=${state.entity}&limit=100`);
  renderCard("Events", JSON.stringify(data.data, null, 2));
}

async function init() {
  const health = await api("/health");
  document.getElementById("db-status").textContent = health.ok ? "DB: ready" : "DB: unavailable";
  await loadRecent();
  renderControls();
  renderEmpty();
}

Array.from(document.querySelectorAll("[data-view]")).forEach((btn) => {
  btn.addEventListener("click", () => setView(btn.dataset.view));
});

Array.from(document.querySelectorAll(".tab-btn")).forEach((btn) => {
  btn.addEventListener("click", () => setView(btn.dataset.tab));
});

const searchBtn = document.getElementById("search-btn");
searchBtn.addEventListener("click", () => {
  const val = globalSearch.value.trim();
  if (val.includes(",")) {
    const [a, b] = val.split(",").map((x) => x.trim());
    if (a && b) {
      state.view = "between";
      api(`/between?a=${a}&b=${b}&limit=200`).then((data) => {
        results.innerHTML = "";
        renderCard("Between", JSON.stringify(data.data, null, 2));
      });
      return;
    }
  }
  state.entity = val || null;
  loadView();
});

document.getElementById("refresh-btn").addEventListener("click", () => loadView());

globalSearch.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    searchBtn.click();
  }
});

init();
