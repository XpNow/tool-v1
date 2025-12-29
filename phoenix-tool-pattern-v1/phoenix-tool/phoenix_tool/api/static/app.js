const api = (path, opts = {}) => fetch(path, opts).then((r) => r.json());

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
const banner = document.getElementById("banner");

const viewConfig = {
  search: { title: "Search", help: "Find events by entity, type, or item." },
  summary: { title: "Summary", help: "Quick overview of totals and top partners." },
  storages: { title: "Storages", help: "Container balances and inventory movement." },
  flow: { title: "Flow", help: "Trace time-coherent chains." },
  trace: { title: "Trace", help: "Network adjacency within a depth." },
  between: { title: "Between", help: "Find events connecting two entities." },
  reports: { title: "Reports", help: "Generate or open reports for an entity." },
  ask: { title: "Ask", help: "Ask a question and review evidence." },
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

  if (state.view === "ask") {
    const textarea = document.createElement("textarea");
    textarea.placeholder = "Ask a question, e.g. 'Show summary for ID 101'";
    textarea.className = "rounded-lg bg-slate-900 border border-slate-700 px-3 py-3 h-28";
    textarea.id = "ask-input";

    const actionBtn = document.createElement("button");
    actionBtn.textContent = "Ask";
    actionBtn.className = "bg-blue-500 hover:bg-blue-600 text-white rounded-lg px-3 py-2";
    actionBtn.addEventListener("click", () => loadView());

    card.appendChild(textarea);
    card.appendChild(actionBtn);
  } else {
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
  }
  controls.appendChild(card);
}

function renderEmpty(message = "Try refining your filters or pick a recent entity.") {
  results.innerHTML = "";
  const empty = document.getElementById("empty-state-template").content.firstElementChild.cloneNode(true);
  empty.querySelector("p").textContent = message;
  results.appendChild(empty);
}

function renderCard(title, bodyHtml) {
  const card = document.getElementById("card-template").content.firstElementChild.cloneNode(true);
  card.innerHTML = `<h3 class="text-lg font-semibold">${title}</h3><div class="text-sm text-slate-200 mt-2 font-mono">${bodyHtml}</div>`;
  results.appendChild(card);
}

function renderLoading() {
  results.innerHTML = "";
  const loading = document.getElementById("loading-template").content.firstElementChild.cloneNode(true);
  results.appendChild(loading);
}

function renderAlert(error) {
  results.innerHTML = "";
  const alert = document.getElementById("alert-template").content.firstElementChild.cloneNode(true);
  alert.querySelector("#alert-title").textContent = error.message || "Something went wrong";
  alert.querySelector("#alert-message").textContent = error.hint || "Try again.";
  const details = alert.querySelector("#alert-details");
  const toggle = alert.querySelector("#alert-toggle");
  if (error.details) {
    details.textContent = error.details;
  } else {
    toggle.classList.add("hidden");
  }
  toggle.addEventListener("click", () => {
    details.classList.toggle("hidden");
    toggle.textContent = details.classList.contains("hidden") ? "Show details" : "Hide details";
  });
  results.appendChild(alert);
}

function renderBanner(message) {
  banner.innerHTML = "";
  if (!message) return;
  const card = document.getElementById("card-template").content.firstElementChild.cloneNode(true);
  card.classList.add("border-amber-500/30", "text-amber-200");
  card.innerHTML = `<div class="font-semibold">Notice</div><div class="text-sm mt-1">${message}</div>`;
  banner.appendChild(card);
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
  renderLoading();
  renderBanner("");
  if (!state.entity && state.view !== "between" && state.view !== "ask") {
    renderEmpty();
    return;
  }

  if (state.view === "summary") {
    const data = await api(`/summary?entity=${state.entity}`);
    if (!data.ok) {
      if (data.error?.code === "EMPTY_DB") {
        renderBanner("DB not built. Click Build DB to run normalize + parse.");
      }
      renderAlert(data.error || {});
      return;
    }
    if (!data.data.events || data.data.events.length === 0) {
      renderEmpty("No summary results. Try a different entity.");
      return;
    }
    renderCard("Summary", JSON.stringify(data.data, null, 2));
    return;
  }

  if (state.view === "storages") {
    const data = await api(`/storages?entity=${state.entity}`);
    if (!data.ok) {
      if (data.error?.code === "EMPTY_DB") {
        renderBanner("DB not built. Click Build DB to run normalize + parse.");
      }
      renderAlert(data.error || {});
      return;
    }
    if (!data.data.containers || data.data.containers.length === 0) {
      renderEmpty("No storage results. Try a different entity.");
      return;
    }
    renderCard("Storages", JSON.stringify(data.data, null, 2));
    return;
  }

  if (state.view === "flow") {
    const data = await api(`/flow?entity=${state.entity}`);
    if (!data.ok) {
      if (data.error?.code === "EMPTY_DB") {
        renderBanner("DB not built. Click Build DB to run normalize + parse.");
      }
      renderAlert(data.error || {});
      return;
    }
    if (!data.data.chains || data.data.chains.length === 0) {
      renderEmpty("No flow results. Try a different entity.");
      return;
    }
    renderCard("Flow", JSON.stringify(data.data, null, 2));
    return;
  }

  if (state.view === "trace") {
    const data = await api(`/trace?entity=${state.entity}`);
    if (!data.ok) {
      if (data.error?.code === "EMPTY_DB") {
        renderBanner("DB not built. Click Build DB to run normalize + parse.");
      }
      renderAlert(data.error || {});
      return;
    }
    if (!data.data.events || data.data.events.length === 0) {
      renderEmpty("No trace results. Try a different entity.");
      return;
    }
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

  if (state.view === "ask") {
    const question = document.getElementById("ask-input")?.value?.trim() || "";
    const data = await api(`/ask?q=${encodeURIComponent(question)}`);
    if (!data.ok) {
      if (data.error?.code === "EMPTY_DB") {
        renderBanner("DB not built. Click Build DB to run normalize + parse.");
      }
      renderAlert(data.error || {});
      return;
    }
    renderCard("Answer", data.data.answer || "No answer available.");

    const evidence = data.data.evidence || [];
    if (evidence.length === 0) {
      renderCard("Evidence", "No evidence returned. Try a narrower question.");
    } else {
      renderCard("Evidence", JSON.stringify(evidence, null, 2));
    }

    const suggested = data.data.suggested_entities || [];
    const primary = data.data.primary_entity || null;
    const suggestedCard = document.getElementById("card-template").content.firstElementChild.cloneNode(true);
    suggestedCard.innerHTML = `<h3 class="text-lg font-semibold">Suggested Actions</h3>`;
    const list = document.createElement("div");
    list.className = "mt-3 flex flex-wrap gap-2";
    suggested.slice(0, 6).forEach((entity) => {
      const btn = document.createElement("button");
      btn.className = "bg-slate-900 border border-slate-700 rounded-lg px-3 py-2 text-sm hover:bg-slate-700";
      btn.textContent = `Open ${entity}`;
      btn.addEventListener("click", () => {
        state.entity = String(entity);
        globalSearch.value = String(entity);
        setView("summary");
        loadView();
      });
      list.appendChild(btn);
    });
    const flowBtn = document.createElement("button");
    flowBtn.className = "bg-blue-500 hover:bg-blue-600 text-white rounded-lg px-3 py-2 text-sm";
    flowBtn.textContent = "Run flow";
    flowBtn.addEventListener("click", () => {
      if (!state.entity) {
        state.entity = primary || suggested[0] || null;
        globalSearch.value = state.entity || "";
      }
      setView("flow");
      loadView();
    });
    const summaryBtn = document.createElement("button");
    summaryBtn.className = "bg-blue-500 hover:bg-blue-600 text-white rounded-lg px-3 py-2 text-sm";
    summaryBtn.textContent = "Run summary";
    summaryBtn.addEventListener("click", () => {
      if (!state.entity) {
        state.entity = primary || suggested[0] || null;
        globalSearch.value = state.entity || "";
      }
      setView("summary");
      loadView();
    });
    list.appendChild(flowBtn);
    list.appendChild(summaryBtn);
    suggestedCard.appendChild(list);
    results.appendChild(suggestedCard);
    return;
  }

  const data = await api(`/events?entity=${state.entity}&limit=100`);
  if (!data.ok) {
    if (data.error?.code === "EMPTY_DB") {
      renderBanner("DB not built. Click Build DB to run normalize + parse.");
    }
    renderAlert(data.error || {});
    return;
  }
  if (!data.data.events || data.data.events.length === 0) {
    renderEmpty("No events returned. Try a different entity or filters.");
    return;
  }
  renderCard("Events", JSON.stringify(data.data, null, 2));
  await loadRecent();
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
      renderLoading();
      api(`/between?a=${a}&b=${b}&limit=200`).then((data) => {
        if (!data.ok) {
          if (data.error?.code === "EMPTY_DB") {
            renderBanner("DB not built. Click Build DB to run normalize + parse.");
          }
          renderAlert(data.error || {});
          return;
        }
        renderCard("Between", JSON.stringify(data.data, null, 2));
      });
      return;
    }
  }
  state.entity = val || null;
  loadView();
});

document.getElementById("refresh-btn").addEventListener("click", () => loadView());

document.getElementById("build-btn").addEventListener("click", async () => {
  renderLoading();
  const data = await api("/build", { method: "POST" });
  if (!data.ok) {
    renderAlert(data.error || {});
    return;
  }
  renderCard("Build Complete", JSON.stringify(data.data, null, 2));
  await loadRecent();
});

globalSearch.addEventListener("keydown", (e) => {
  if (e.key === "Enter") {
    searchBtn.click();
  }
});

init();
