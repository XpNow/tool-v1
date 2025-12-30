const state = {
  currentView: "home",
  currentEntity: null,
  recentEntities: [],
  lastResponse: null,
  searchParams: {},
};

const viewTitle = document.getElementById("view-title");
const viewContent = document.getElementById("view-content");
const recentEntitiesEl = document.getElementById("recent-entities");
const globalAskInput = document.getElementById("global-ask-input");
const globalAskBtn = document.getElementById("global-ask-btn");
const dbStatus = document.getElementById("db-status");
const buildBtn = document.getElementById("build-db-btn");

function setStatus(text, tone) {
  dbStatus.textContent = text;
  dbStatus.className = `rounded-full border px-3 py-1 text-xs ${
    tone === "ok"
      ? "border-emerald-400/40 text-emerald-200"
      : tone === "warn"
      ? "border-orange-400/40 text-orange-200"
      : "border-red-500/50 text-red-300"
  }`;
}

async function fetchJson(url, options = {}) {
  try {
    const response = await fetch(url, options);
    const data = await response.json();
    return data;
  } catch (err) {
    return {
      ok: false,
      error: {
        code: "INTERNAL",
        message: "Network error.",
        hint: "Check the API server and retry.",
        details: err.message,
      },
    };
  }
}

function renderLoading(message = "Loading…") {
  viewContent.innerHTML = `
    <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/60 p-6 text-slate-300">
      ${message}
    </div>
  `;
}

function renderError(error) {
  viewContent.innerHTML = `
    <div class="rounded-xl border border-red-500/40 bg-red-500/10 p-6">
      <div class="text-sm uppercase tracking-wide text-red-200">Error</div>
      <div class="mt-2 text-lg font-semibold">${error.message || "Something went wrong."}</div>
      <div class="mt-1 text-sm text-red-200/80">${error.hint || "Check inputs and retry."}</div>
      <details class="mt-4 text-xs text-red-200/70">
        <summary class="cursor-pointer">Details</summary>
        <pre class="mt-2 whitespace-pre-wrap">${error.details || "No additional details."}</pre>
      </details>
    </div>
  `;
}

function renderErrorFromResponse(data) {
  if (data?.error) {
    renderError(data.error);
    return;
  }
  if (data?.detail) {
    renderError({
      message: "Validation error.",
      hint: "Check inputs and retry.",
      details: JSON.stringify(data.detail, null, 2),
    });
    return;
  }
  renderError({ message: "Something went wrong.", hint: "Check inputs and retry.", details: "No additional details." });
}

function renderEmpty(message, hint) {
  viewContent.innerHTML = `
    <div class="rounded-xl border border-orange-400/30 bg-orange-400/10 p-6">
      <div class="text-sm uppercase tracking-wide text-orange-200">Empty</div>
      <div class="mt-2 text-lg font-semibold">${message}</div>
      <div class="mt-1 text-sm text-orange-100/70">${hint || "Try another filter or ingest more data."}</div>
    </div>
  `;
}

function renderWarnings(warnings = []) {
  if (!warnings.length) return "";
  return `
    <div class="mb-4 rounded-xl border border-orange-400/40 bg-orange-400/10 p-4 text-sm text-orange-100">
      <div class="font-semibold text-orange-200">Warnings</div>
      <ul class="mt-2 list-disc space-y-1 pl-5">
        ${warnings.map((w) => `<li>${w.message}</li>`).join("")}
      </ul>
    </div>
  `;
}

function renderRawToggle(payload) {
  return `
    <div class="mt-6 rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
      <label class="flex items-center gap-2 text-sm text-slate-300">
        <input type="checkbox" class="raw-toggle h-4 w-4 rounded border-orange-900/50 bg-[#1a0b0b]" />
        View raw JSON
      </label>
      <pre class="raw-json mt-3 hidden max-h-64 overflow-auto rounded-lg bg-[#1a0b0b] p-3 text-xs text-slate-200">${JSON.stringify(
        payload,
        null,
        2,
      )}</pre>
    </div>
  `;
}

function attachRawToggle() {
  const toggle = viewContent.querySelector(".raw-toggle");
  const raw = viewContent.querySelector(".raw-json");
  if (!toggle || !raw) return;
  toggle.addEventListener("change", () => {
    raw.classList.toggle("hidden", !toggle.checked);
  });
}

function renderHome() {
  viewTitle.textContent = "Home";
  viewContent.innerHTML = `
    <div class="grid gap-6 md:grid-cols-2 xl:grid-cols-3">
      ${[
        ["Search", "Search entities, items, and timelines.", "search"],
        ["Ask", "Natural language to jump-start investigations.", "ask"],
        ["Summary", "Quick overview of activity and partners.", "summary"],
        ["Storages", "Container inventory with negative highlights.", "storages"],
        ["Flow", "Trace item and money flows.", "flow"],
        ["Trace", "Graph-style trace paths.", "trace"],
        ["Between", "Events connecting two entities.", "between"],
        ["Reports", "Generate professional case exports.", "reports"],
      ]
        .map(
          ([title, desc, view]) => `
          <button data-view="${view}" class="tile-card group rounded-2xl border border-orange-700/50 bg-[#2b1111]/70 p-6 text-left transition duration-150 hover:scale-[1.02] hover:border-orange-300/70 hover:shadow-glow">
            <div class="text-xs uppercase tracking-wide text-slate-400">${view}</div>
            <div class="mt-3 text-lg font-semibold text-slate-100">${title}</div>
            <div class="mt-2 text-sm text-slate-300">${desc}</div>
          </button>
        `,
        )
        .join("")}
    </div>
  `;
  viewContent.querySelectorAll(".tile-card").forEach((btn) => {
    btn.addEventListener("click", () => setView(btn.dataset.view));
  });
}

function renderFormCard(title, bodyHtml, actionLabel = "Run") {
  return `
    <div class="rounded-2xl border border-orange-900/40 bg-[#2b1111]/70 p-6">
      <div class="text-sm uppercase tracking-wide text-slate-400">${title}</div>
      <div class="mt-4 space-y-4">${bodyHtml}</div>
      <button class="action-btn mt-4 rounded-lg bg-orange-500 px-4 py-2 text-sm font-semibold text-slate-900 hover:bg-orange-400">
        ${actionLabel}
      </button>
    </div>
  `;
}

function applyFieldStyles() {
  viewContent.querySelectorAll(".field").forEach((field) => {
    field.classList.add(
      "w-full",
      "rounded-lg",
      "border",
      "border-orange-900/40",
      "bg-[#1a0b0b]",
      "px-3",
      "py-2",
      "text-sm",
      "text-slate-200",
      "placeholder:text-slate-500",
      "focus:border-orange-400",
      "focus:outline-none",
    );
  });
}

function renderEntityRequiredEmpty() {
  renderEmpty("No entity selected.", "Pick a recent entity, or use Search to select one.");
}

function renderSearchView() {
  viewTitle.textContent = "Search";
  const entityValue = state.currentEntity || "";
  const defaults = {
    entity: state.searchParams.entity || entityValue,
    name: state.searchParams.name || "",
    item: state.searchParams.item || "",
    type: state.searchParams.type || "",
    from: state.searchParams.from || "",
    to: state.searchParams.to || "",
    limit: state.searchParams.limit || "200",
  };
  viewContent.innerHTML = renderFormCard(
    "Search filters",
    `
    <div class="grid gap-4 md:grid-cols-2">
      <input class="field" placeholder="Entity ID" value="${defaults.entity}" data-field="entity" />
      <input class="field" placeholder="Name" value="${defaults.name}" data-field="name" />
      <input class="field" placeholder="Item" value="${defaults.item}" data-field="item" />
      <input class="field" placeholder="Type (bank_transfer, oferă, etc.)" value="${defaults.type}" data-field="type" />
      <input class="field" placeholder="From (ISO)" value="${defaults.from}" data-field="from" />
      <input class="field" placeholder="To (ISO)" value="${defaults.to}" data-field="to" />
      <input class="field" placeholder="Limit (default 200)" value="${defaults.limit}" data-field="limit" />
    </div>
  `,
  );
  applyFieldStyles();
  const actionBtn = viewContent.querySelector(".action-btn");
  actionBtn.addEventListener("click", async () => {
    await runSearch(0);
  });
}

async function runSearch(offset) {
  const params = Object.keys(state.searchParams).length ? { ...state.searchParams } : {};
  const freshParams = collectFields();
  if (Object.keys(freshParams).length) {
    Object.assign(params, freshParams);
  }
  renderLoading("Running search…");
  const limit = Number(params.limit || 200);
  state.searchParams = { ...params, limit: String(limit), offset: String(offset) };
  const query = new URLSearchParams({ ...params, limit, offset }).toString();
  const data = await fetchJson(`/search?${query}`);
  state.lastResponse = data;
  if (!data.ok) {
    renderErrorFromResponse(data);
    attachRawToggle();
    return;
  }
  const rows = data.data?.events || [];
  if (!rows.length) {
    renderEmpty("No events returned.", "Adjust filters or pick another entity.");
    viewContent.innerHTML += renderRawToggle(data);
    attachRawToggle();
    return;
  }
  const matched = data.data.matched_total ?? 0;
  const returned = data.data.returned_count ?? rows.length;
  const currentOffset = data.data.offset ?? offset;
  const hasNext = returned >= limit;
  viewContent.innerHTML = `
    ${renderWarnings(data.warnings)}
    <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
      <div class="mb-3 flex flex-wrap items-center justify-between gap-3 text-sm text-slate-300">
        <div>Matched: ${matched}</div>
        <div>Showing ${currentOffset + 1}–${currentOffset + returned}</div>
      </div>
      <div class="overflow-auto">
        <table class="w-full text-left text-sm">
          <thead class="text-xs uppercase text-slate-400">
            <tr>
              <th class="py-2">Timestamp</th>
              <th>Type</th>
              <th>Source</th>
              <th>Target</th>
              <th>Item</th>
              <th>Money</th>
            </tr>
          </thead>
          <tbody class="text-slate-200">
            ${rows
              .map(
                (row) => `
              <tr class="border-t border-orange-900/30">
                <td class="py-2">${row.ts || "—"}</td>
                <td>${row.event_type || "—"}</td>
                <td>${row.src_id || row.src_name || "—"}</td>
                <td>${row.dst_id || row.dst_name || "—"}</td>
                <td>${row.item || "—"}</td>
                <td>${row.money ?? "—"}</td>
              </tr>
            `,
              )
              .join("")}
          </tbody>
        </table>
      </div>
      <div class="mt-4 flex items-center gap-3">
        <button class="page-btn prev-btn" ${currentOffset <= 0 ? "disabled" : ""}>Previous</button>
        <button class="page-btn next-btn" ${hasNext ? "" : "disabled"}>Next</button>
      </div>
    </div>
    ${renderRawToggle(data)}
  `;
  attachRawToggle();
  viewContent.querySelectorAll(".page-btn").forEach((btn) => {
    btn.className =
      "page-btn rounded-lg border border-orange-900/40 bg-[#1a0b0b] px-4 py-2 text-xs text-slate-200 hover:border-orange-400 disabled:cursor-not-allowed disabled:opacity-50";
  });
  const prevBtn = viewContent.querySelector(".prev-btn");
  const nextBtn = viewContent.querySelector(".next-btn");
  if (prevBtn) {
    prevBtn.addEventListener("click", () => runSearch(Math.max(currentOffset - limit, 0)));
  }
  if (nextBtn) {
    nextBtn.addEventListener("click", () => runSearch(currentOffset + limit));
  }
}

function renderSummaryView() {
  viewTitle.textContent = "Summary";
  const entityValue = state.currentEntity || "";
  viewContent.innerHTML = renderFormCard(
    "Summary",
    `
      <input class="field" placeholder="Entity ID" value="${entityValue}" data-field="entity" />
  `,
  );
  applyFieldStyles();
  viewContent.querySelector(".action-btn").addEventListener("click", async () => {
    const params = collectFields();
    if (!params.entity) {
      renderEntityRequiredEmpty();
      return;
    }
    renderLoading("Building summary…");
    const data = await fetchJson(`/summary?entity=${encodeURIComponent(params.entity || "")}`);
    state.lastResponse = data;
    if (!data.ok) {
      renderErrorFromResponse(data);
      attachRawToggle();
      return;
    }
    const summary = data.data;
    if (!summary?.events?.length) {
      renderEmpty("No summary events found.", "Run search or ingest more data.");
      viewContent.innerHTML += renderRawToggle(data);
      attachRawToggle();
      return;
    }
    viewContent.innerHTML = `
      ${renderWarnings(data.warnings)}
      <div class="grid gap-4 md:grid-cols-3">
        <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
          <div class="text-xs uppercase text-slate-400">Money In</div>
          <div class="mt-2 text-xl font-semibold">${summary.money_in_formatted || summary.money_in || "—"}</div>
        </div>
        <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
          <div class="text-xs uppercase text-slate-400">Money Out</div>
          <div class="mt-2 text-xl font-semibold">${summary.money_out_formatted || summary.money_out || "—"}</div>
        </div>
        <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
          <div class="text-xs uppercase text-slate-400">Events</div>
          <div class="mt-2 text-xl font-semibold">${summary.events.length}</div>
        </div>
      </div>
      <div class="mt-6 grid gap-4 md:grid-cols-2">
        <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
          <div class="text-sm text-slate-300">Top Partners</div>
          <ul class="mt-3 space-y-2 text-sm text-slate-200">
            ${(summary.top_partners || [])
              .map(
                (p) =>
                  `<li class="flex items-center justify-between"><span>${p.partner_name || p.partner_id}</span><span class="text-slate-400">${p.count}</span></li>`,
              )
              .join("")}
          </ul>
        </div>
        <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
          <div class="text-sm text-slate-300">Event Counts</div>
          <ul class="mt-3 space-y-2 text-sm text-slate-200">
            ${(summary.event_counts || [])
              .map((c) => `<li class="flex items-center justify-between"><span>${c[0]}</span><span class="text-slate-400">${c[1]}</span></li>`)
              .join("")}
          </ul>
        </div>
      </div>
      ${renderRawToggle(data)}
    `;
    attachRawToggle();
  });
}

function renderStoragesView() {
  viewTitle.textContent = "Storages";
  const entityValue = state.currentEntity || "";
  viewContent.innerHTML = renderFormCard(
    "Storage filters",
    `
      <div class="grid gap-4 md:grid-cols-2">
        <input class="field" placeholder="Entity ID" value="${entityValue}" data-field="entity" />
        <input class="field" placeholder="Container (optional)" data-field="container" />
        <input class="field" placeholder="From (ISO)" data-field="from" />
        <input class="field" placeholder="To (ISO)" data-field="to" />
      </div>
  `,
  );
  applyFieldStyles();
  viewContent.querySelector(".action-btn").addEventListener("click", async () => {
    const params = collectFields();
    if (!params.entity) {
      renderEntityRequiredEmpty();
      return;
    }
    renderLoading("Loading containers…");
    const query = new URLSearchParams(params).toString();
    const data = await fetchJson(`/storages?${query}`);
    state.lastResponse = data;
    if (!data.ok) {
      renderErrorFromResponse(data);
      attachRawToggle();
      return;
    }
    const containers = data.data?.containers || [];
    if (!containers.length) {
      renderEmpty("No storage activity found.", "Try another entity or time range.");
      viewContent.innerHTML += renderRawToggle(data);
      attachRawToggle();
      return;
    }
    viewContent.innerHTML = `
      ${renderWarnings(data.warnings)}
      <div class="space-y-6">
        ${containers
          .map(
            (c) => `
            <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
              <div class="flex items-center justify-between">
                <div class="text-lg font-semibold">${c.container}</div>
                <div class="text-xs text-slate-400">puts ${c.puts} · removes ${c.removes}</div>
              </div>
              <div class="mt-4 overflow-auto">
                <table class="w-full text-left text-sm">
                  <thead class="text-xs uppercase text-slate-400">
                    <tr>
                      <th class="py-2">Item</th>
                      <th>Current</th>
                      <th>Total In</th>
                      <th>Total Out</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${c.items
                      .map(
                        (item) => `
                      <tr class="border-t border-orange-900/30">
                        <td class="py-2">${item.item}</td>
                        <td class="${item.current < 0 ? "text-red-300" : "text-slate-200"}">${item.current}</td>
                        <td class="text-slate-300">${item.total_in}</td>
                        <td class="text-slate-300">${item.total_out}</td>
                      </tr>
                    `,
                      )
                      .join("")}
                  </tbody>
                </table>
              </div>
            </div>
          `,
          )
          .join("")}
      </div>
      ${renderRawToggle(data)}
    `;
    attachRawToggle();
  });
}

function renderFlowView() {
  viewTitle.textContent = "Flow";
  const entityValue = state.currentEntity || "";
  viewContent.innerHTML = renderFormCard(
    "Flow settings",
    `
      <div class="grid gap-4 md:grid-cols-2">
        <input class="field" placeholder="Entity ID" value="${entityValue}" data-field="entity" />
        <select class="field" data-field="direction">
          <option value="both">Both</option>
          <option value="out">Out</option>
          <option value="in">In</option>
        </select>
        <input class="field" placeholder="Depth (default 4)" data-field="depth" />
        <input class="field" placeholder="Window minutes (default 120)" data-field="window" />
        <input class="field" placeholder="Item filter" data-field="item" />
      </div>
  `,
  );
  applyFieldStyles();
  viewContent.querySelector(".action-btn").addEventListener("click", async () => {
    const params = collectFields();
    if (!params.entity) {
      renderEntityRequiredEmpty();
      return;
    }
    renderLoading("Tracing flow…");
    const query = new URLSearchParams(params).toString();
    const data = await fetchJson(`/flow?${query}`);
    state.lastResponse = data;
    if (!data.ok) {
      renderErrorFromResponse(data);
      attachRawToggle();
      return;
    }
    const chains = data.data?.chains || [];
    if (!chains.length) {
      renderEmpty("No flow chains found.", "Try another entity or expand depth.");
      viewContent.innerHTML += renderRawToggle(data);
      attachRawToggle();
      return;
    }
    viewContent.innerHTML = `
      <div class="mb-4 flex gap-3">
        <button class="export-btn" data-format="json">Export JSON</button>
        <button class="export-btn" data-format="txt">Export TXT</button>
      </div>
      <div class="space-y-4">
        ${chains
          .map(
            (chain) => `
            <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
              <div class="text-xs uppercase text-slate-400">${chain.direction}</div>
              <div class="mt-2 text-sm text-slate-200">Steps: ${chain.chain.length}</div>
            </div>
          `,
          )
          .join("")}
      </div>
      ${renderRawToggle(data)}
    `;
    attachRawToggle();
    attachExportButtons(data);
  });
}

function renderTraceView() {
  viewTitle.textContent = "Trace";
  const entityValue = state.currentEntity || "";
  if (!entityValue) {
    renderEntityRequiredEmpty();
    return;
  }
  viewContent.innerHTML = renderFormCard(
    "Trace settings",
    `
      <div class="grid gap-4 md:grid-cols-2">
        <input class="field" placeholder="Entity ID" value="${entityValue}" data-field="entity" />
        <input class="field" placeholder="Depth (default 2)" data-field="depth" />
        <input class="field" placeholder="Item filter" data-field="item" />
      </div>
  `,
  );
  applyFieldStyles();
  viewContent.querySelector(".action-btn").addEventListener("click", async () => {
    const params = collectFields();
    renderLoading("Tracing graph…");
    const query = new URLSearchParams(params).toString();
    const data = await fetchJson(`/trace?${query}`);
    state.lastResponse = data;
    if (!data.ok) {
      renderErrorFromResponse(data);
      attachRawToggle();
      return;
    }
    const events = data.data?.events || [];
    if (!events.length) {
      renderEmpty("No trace path found.", "Try another entity or increase depth.");
      viewContent.innerHTML += renderRawToggle(data);
      attachRawToggle();
      return;
    }
    viewContent.innerHTML = `
      <div class="mb-4 flex gap-3">
        <button class="export-btn" data-format="json">Export JSON</button>
        <button class="export-btn" data-format="txt">Export TXT</button>
      </div>
      <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
        <div class="text-sm text-slate-300">Nodes: ${(data.data.nodes || []).length}</div>
        <ul class="mt-3 space-y-2 text-sm text-slate-200">
          ${events
            .slice(0, 20)
            .map(
              (e) =>
                `<li>${e.ts || "—"} · ${e.event_type || "—"} · ${e.src_id || e.src_name || "—"} → ${
                  e.dst_id || e.dst_name || "—"
                }</li>`,
            )
            .join("")}
        </ul>
      </div>
      ${renderRawToggle(data)}
    `;
    attachRawToggle();
    attachExportButtons(data);
  });
}

function renderBetweenView() {
  viewTitle.textContent = "Between";
  if (!state.currentEntity) {
    renderEntityRequiredEmpty();
    return;
  }
  viewContent.innerHTML = renderFormCard(
    "Between entities",
    `
      <div class="grid gap-4 md:grid-cols-2">
        <input class="field" placeholder="Entity A" data-field="a" />
        <input class="field" placeholder="Entity B" data-field="b" />
        <input class="field" placeholder="From (ISO)" data-field="from" />
        <input class="field" placeholder="To (ISO)" data-field="to" />
      </div>
  `,
  );
  applyFieldStyles();
  viewContent.querySelector(".action-btn").addEventListener("click", async () => {
    const params = collectFields();
    renderLoading("Finding connections…");
    const query = new URLSearchParams(params).toString();
    const data = await fetchJson(`/between?${query}`);
    state.lastResponse = data;
    if (!data.ok) {
      renderErrorFromResponse(data);
      attachRawToggle();
      return;
    }
    const events = data.data?.events || [];
    if (!events.length) {
      renderEmpty("No connecting events found.", "Try another pair or broaden the time range.");
      viewContent.innerHTML += renderRawToggle(data);
      attachRawToggle();
      return;
    }
    viewContent.innerHTML = `
      <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
        <div class="text-sm text-slate-300">Connections: ${data.data.matched_total}</div>
        <ul class="mt-3 space-y-2 text-sm text-slate-200">
          ${events
            .slice(0, 20)
            .map((e) => `<li>${e.ts || "—"} · ${e.event_type || "—"} · ${e.src_id} → ${e.dst_id}</li>`)
            .join("")}
        </ul>
      </div>
      ${renderRawToggle(data)}
    `;
    attachRawToggle();
  });
}

function renderAskView(question = "") {
  viewTitle.textContent = "Ask Phoenix";
  const emptyNotice = !question
    ? `
      <div class="mb-4 rounded-xl border border-orange-400/30 bg-orange-400/10 p-4">
        <div class="text-sm uppercase tracking-wide text-orange-200">Empty</div>
        <div class="mt-2 text-lg font-semibold">No question submitted.</div>
        <div class="mt-1 text-sm text-orange-100/70">Ask Phoenix using the global bar or type a question below.</div>
      </div>
    `
    : "";
  viewContent.innerHTML = `
    ${emptyNotice}
    ${renderFormCard(
      "Ask Phoenix",
      `
        <input class="field" placeholder="Your question" value="${question}" data-field="question" />
    `,
      "Ask",
    )}
  `;
  applyFieldStyles();
  viewContent.querySelector(".action-btn").addEventListener("click", async () => {
    const params = collectFields();
    if (!params.question) {
      renderEmpty("No question submitted.", "Ask Phoenix using the global bar or type a question below.");
      return;
    }
    renderLoading("Analyzing…");
    const data = await fetchJson(`/ask?q=${encodeURIComponent(params.question || "")}`);
    state.lastResponse = data;
    if (!data.ok) {
      renderErrorFromResponse(data);
      attachRawToggle();
      return;
    }
    const payload = data.data || {};
    viewContent.innerHTML = `
      <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-6">
        <div class="text-sm uppercase tracking-wide text-slate-400">Answer</div>
        <div class="mt-3 text-lg font-semibold">Intent: ${payload.intent || "ask"}</div>
        <div class="mt-2 text-sm text-slate-300">Primary entity: ${payload.pid || "—"}</div>
      </div>
      <div class="mt-6 grid gap-4 md:grid-cols-2">
        <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
          <div class="text-sm text-slate-300">Evidence</div>
          <ul class="mt-3 space-y-2 text-sm text-slate-200">
            ${(payload.data?.events || [])
              .slice(0, 8)
              .map((e) => `<li>${e.ts || "—"} · ${e.event_type || "—"}</li>`)
              .join("")}
          </ul>
        </div>
        <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-4">
          <div class="text-sm text-slate-300">Suggested next actions</div>
          <ul class="mt-3 space-y-2 text-sm text-slate-200">
            ${(payload.data?.nodes || [])
              .slice(0, 8)
              .map((n) => `<li>Inspect ${n}</li>`)
              .join("")}
          </ul>
        </div>
      </div>
      ${renderRawToggle(data)}
    `;
    attachRawToggle();
  });
}

function renderReportsView() {
  viewTitle.textContent = "Reports";
  if (!state.currentEntity) {
    renderEntityRequiredEmpty();
    return;
  }
  const placeholder = {
    ok: true,
    data: {
      note: "Report generation is available via CLI. UI export workflows are coming soon.",
    },
  };
  viewContent.innerHTML = `
    <div class="rounded-xl border border-orange-900/40 bg-[#2b1111]/70 p-6">
      <div class="text-sm uppercase tracking-wide text-slate-400">Reports</div>
      <div class="mt-3 text-lg font-semibold">Professional exports</div>
      <div class="mt-2 text-sm text-slate-300">
        Generate case files via <span class="text-orange-300">report &lt;id&gt;</span> in the CLI. This UI will add
        download options in a future release.
      </div>
    </div>
    ${renderRawToggle(placeholder)}
  `;
  attachRawToggle();
}

function collectFields() {
  const fields = viewContent.querySelectorAll(".field");
  const params = {};
  fields.forEach((field) => {
    const key = field.dataset.field;
    if (!key) return;
    const value = field.value.trim();
    if (value) {
      params[key] = value;
    }
  });
  return params;
}

function attachExportButtons(data) {
  viewContent.querySelectorAll(".export-btn").forEach((btn) => {
    btn.className =
      "export-btn rounded-lg border border-orange-900/40 bg-[#1a0b0b] px-3 py-2 text-xs text-slate-200 hover:border-orange-400";
    btn.addEventListener("click", () => {
      const format = btn.dataset.format;
      const payload = format === "txt" ? JSON.stringify(data.data, null, 2) : JSON.stringify(data, null, 2);
      const blob = new Blob([payload], { type: "text/plain;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `phoenix_${state.currentView}.${format === "txt" ? "txt" : "json"}`;
      link.click();
      URL.revokeObjectURL(url);
    });
  });
}

function setView(view) {
  state.currentView = view;
  switch (view) {
    case "home":
      renderHome();
      break;
    case "search":
      renderSearchView();
      break;
    case "summary":
      renderSummaryView();
      break;
    case "storages":
      renderStoragesView();
      break;
    case "flow":
      renderFlowView();
      break;
    case "trace":
      renderTraceView();
      break;
    case "between":
      renderBetweenView();
      break;
    case "reports":
      renderReportsView();
      break;
    case "ask":
      renderAskView();
      break;
    default:
      renderHome();
  }
}

function updateRecentEntities() {
  if (!state.recentEntities.length) {
    recentEntitiesEl.innerHTML = `<div class="text-slate-500">No recent entities.</div>`;
    return;
  }
  recentEntitiesEl.innerHTML = state.recentEntities
    .map(
      (e) => `
      <button class="entity-btn w-full rounded-lg border border-orange-900/40 bg-[#2b1111]/60 px-3 py-2 text-left text-xs text-slate-200 hover:border-orange-300" data-entity="${e.player_id}">
        <div class="font-semibold">${e.player_id}</div>
        <div class="text-[11px] text-slate-400">${e.name || "Unknown"} · ${e.last_seen || "—"}</div>
      </button>
    `,
    )
    .join("");

  recentEntitiesEl.querySelectorAll(".entity-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.currentEntity = btn.dataset.entity;
      setView("summary");
    });
  });
}

async function boot() {
  document.querySelectorAll(".sidebar-btn").forEach((btn) => {
    btn.className =
      "sidebar-btn w-full rounded-lg border border-transparent px-3 py-2 text-left text-sm text-slate-300 hover:border-orange-900/40 hover:bg-[#2b1111]";
    btn.addEventListener("click", () => setView(btn.dataset.view));
  });

  globalAskBtn.addEventListener("click", () => {
    const q = globalAskInput.value.trim();
    state.currentView = "ask";
    renderAskView(q);
  });

  buildBtn.addEventListener("click", async () => {
    buildBtn.disabled = true;
    buildBtn.textContent = "Building…";
    const data = await fetchJson("/build", { method: "POST" });
    if (!data.ok) {
      setStatus("Build failed", "error");
      renderErrorFromResponse(data);
      attachRawToggle();
    } else {
      setStatus("DB rebuilt", "ok");
    }
    buildBtn.disabled = false;
    buildBtn.textContent = "Build DB";
  });

  const health = await fetchJson("/health");
  if (health.ok) {
    setStatus("API connected", "ok");
  } else {
    setStatus("API offline", "error");
  }

  const entities = await fetchJson("/entities?limit=8");
  if (entities.ok) {
    state.recentEntities = entities.data?.entities || [];
  } else {
    state.recentEntities = [];
  }
  updateRecentEntities();
  renderHome();
}

boot();
