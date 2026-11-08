const $ = (id) => document.getElementById(id);

const finalStates = new Set(["succeeded", "completed_with_errors", "failed"]);
const importLabels = {
  queued: "În așteptare",
  running: "În curs",
  succeeded: "Finalizat cu succes",
  completed_with_errors: "Finalizat cu rânduri respinse",
  failed: "Eșuat",
};

function message(text, error = false) {
  $("message").textContent = text;
  $("message").dataset.kind = error ? "error" : "info";
}

function showWebhook(status = "idle", attempts = 0) {
  const labels = {
    idle: "Se trimite după finalizarea importului.",
    pending: "Raportul este gata. Se așteaptă confirmarea livrării.",
    delivered: "Webhook livrat receptorului local.",
    failed: `Livrarea a eșuat. Încercări efectuate: ${attempts}.`,
  };

  const text = labels[status] ?? labels.idle;
  $("hook-box").dataset.status = status;
  if ($("hook").textContent !== text) $("hook").textContent = text;
}

async function request(url, options) {
  const response = await fetch(url, options);
  const data = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(data?.error || `Serverul a răspuns cu HTTP ${response.status}.`);
  }

  if (data === null) throw new Error("Serverul nu a returnat un răspuns JSON.");
  return { response, data };
}

function showImport(job) {
  const finished = finalStates.has(job.status);
  $("state").textContent = `Stare: ${importLabels[job.status] ?? job.status}`;

  const bar = $("bar");
  if (job.totalRows === null && !finished) {
    bar.removeAttribute("value");
  } else {
    bar.max = Math.max(job.totalRows ?? 0, 1);
    bar.value = job.processedRows;
  }

  $("counts").textContent = job.totalRows === null
    ? (finished ? `Rânduri verificate: ${job.processedRows}.` : "Se pregătește fișierul…")
    : `Rânduri verificate: ${job.processedRows} din ${job.totalRows}.`;

  $("saved").hidden = !finished;
  $("saved").textContent = `Produse salvate în acest import: ${job.savedRows}.`;

  const items = job.errors.map((error) => {
    const item = document.createElement("li");
    const row = error.row ? `Rândul ${error.row}${error.sku ? ` (${error.sku})` : ""}: ` : "";
    item.textContent = row + error.message;
    return item;
  });
  $("errors").replaceChildren(...items);

  showWebhook(job.webhook.status ?? "idle", job.webhook.attempts);

  if (finished) {
    $("report").hidden = false;
    $("raw").textContent = JSON.stringify(job, null, 2);
  }
}

function showError(error) {
  const text = error instanceof TypeError
    ? "Conexiunea cu API-ul a eșuat. Verifică dacă server.py rulează pe portul 8000."
    : error.message;
  message(text, true);
  $("submit").disabled = false;
}

async function poll(statusUrl) {
  try {
    const { data: job } = await request(statusUrl);
    showImport(job);

    if (!finalStates.has(job.status) || job.webhook.status === "pending") {
      setTimeout(() => poll(statusUrl), 700);
    } else {
      $("submit").disabled = false;
    }
  } catch (error) {
    showError(error);
  }
}

function watchSse(statusUrl) {
  const stream = new EventSource(`${statusUrl}/events`);
  let active = true;

  function closeStream() {
    active = false;
    stream.close();
  }

  stream.onmessage = (event) => {
    if (!active) return;
    const job = JSON.parse(event.data);
    showImport(job);

    if (finalStates.has(job.status)) {
      closeStream();
      // Receptorul poate confirma livrarea după ce importul s-a încheiat.
      if (job.webhook.status === "pending") poll(statusUrl);
      else $("submit").disabled = false;
    }
  };

  stream.onerror = () => {
    if (!active) return;
    closeStream();
    message("Fluxul SSE s-a întrerupt. Verific starea prin polling.");
    poll(statusUrl);
  };
}

$("import-form").addEventListener("submit", async (event) => {
  event.preventDefault();

  if (location.protocol === "file:") {
    message("Pornește server.py și deschide http://127.0.0.1:8000/.", true);
    return;
  }

  const file = $("file").files[0];
  if (!file) return;

  const mode = $("mode").value;
  const channel = $("channel").value;

  $("submit").disabled = true;
  $("state").textContent = "Stare: se trimite fișierul";
  $("counts").textContent = "Rânduri verificate: —";
  $("bar").value = 0;
  $("saved").hidden = true;
  $("errors").replaceChildren();
  $("report").hidden = true;
  $("report").open = false;
  showWebhook();
  message("Se trimite fișierul…");

  try {
    const { response, data } = await request(`/imports?mode=${encodeURIComponent(mode)}`, {
      method: "POST",
      headers: { "Content-Type": "text/csv" },
      body: file,
    });

    if (response.status !== 202 || !data.statusUrl) {
      throw new Error("Răspunsul API-ului nu conține adresa importului.");
    }

    message(`202 Accepted · Urmărire prin ${channel === "sse" ? "SSE" : "polling"}`);
    $("state").textContent = "Stare: În așteptare";

    if (channel === "sse") watchSse(data.statusUrl);
    else poll(data.statusUrl);
  } catch (error) {
    showError(error);
  }
});