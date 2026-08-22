document.addEventListener('DOMContentLoaded', () => {
  const authModal = document.getElementById('authModal');
  const authForm = document.getElementById('authForm');
  const tokenInput = document.getElementById('tokenInput');
  const authError = document.getElementById('authError');
  const logoutBtn = document.getElementById('logoutBtn');
  const triggerBtn = document.getElementById('triggerBtn');
  const logConsole = document.getElementById('logConsole');
  const apiResultJson = document.getElementById('apiResultJson');

  const totalEmpresas = document.getElementById('totalEmpresas');
  const totalEstab = document.getElementById('totalEstab');
  const totalSocios = document.getElementById('totalSocios');
  const ultimaCompetencia = document.getElementById('ultimaCompetencia');

  let apiToken = localStorage.getItem('cnpj_api_token') || '';
  let activeSSE = null;
  let statsIntervalId = null;

  if (apiToken) {
    verifyToken(apiToken);
  } else {
    showModal();
  }

  authForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const token = tokenInput.value.trim();
    if (!token) return;

    authError.textContent = '';
    const success = await verifyToken(token);
    if (success) {
      apiToken = token;
      localStorage.setItem('cnpj_api_token', token);
      hideModal();
    } else {
      authError.textContent = 'Token inválido. Verifique o API_TOKEN no arquivo .env';
    }
  });

  logoutBtn.addEventListener('click', () => {
    localStorage.removeItem('cnpj_api_token');
    apiToken = '';
    if (activeSSE) {
      activeSSE.close();
      activeSSE = null;
    }
    if (statsIntervalId) {
      clearInterval(statsIntervalId);
      statsIntervalId = null;
    }
    showModal();
  });

  triggerBtn.addEventListener('click', async () => {
    if (!apiToken) return;
    try {
      const res = await fetch('/api/v1/trigger-etl', {
        method: 'POST',
        headers: { 'X-API-Token': apiToken }
      });
      const data = await res.json();
      appendLog(`[Trigger] ${data.message || 'Verificação disparada com sucesso.'}`);
    } catch (err) {
      appendLog(`[Trigger Error] ${err.message}`);
    }
  });

  // Tab controls
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
      btn.classList.add('active');
      document.getElementById(btn.dataset.tab).classList.add('active');
    });
  });

  // Search CNPJ
  document.getElementById('btnSearchCnpj').addEventListener('click', async () => {
    const cnpj = document.getElementById('searchCnpjInput').value.trim();
    if (!cnpj) return;

    try {
      const res = await fetch(`/api/v1/cnpj/${encodeURIComponent(cnpj)}`, {
        headers: { 'X-API-Token': apiToken }
      });
      const data = await res.json();
      apiResultJson.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
      apiResultJson.textContent = JSON.stringify({ error: err.message }, null, 2);
    }
  });

  // Search Query
  document.getElementById('btnSearchQuery').addEventListener('click', async () => {
    const q = document.getElementById('searchQueryInput').value.trim();
    const uf = document.getElementById('searchUfInput').value.trim();
    if (!q) return;

    try {
      const url = `/api/v1/busca?q=${encodeURIComponent(q)}&uf=${encodeURIComponent(uf)}`;
      const res = await fetch(url, {
        headers: { 'X-API-Token': apiToken }
      });
      const data = await res.json();
      apiResultJson.textContent = JSON.stringify(data, null, 2);
    } catch (err) {
      apiResultJson.textContent = JSON.stringify({ error: err.message }, null, 2);
    }
  });

  async function verifyToken(token) {
    try {
      const res = await fetch('/api/v1/status', {
        headers: { 'X-API-Token': token }
      });
      if (res.ok) {
        const data = await res.json();
        updateStats(data.stats);
        hideModal();
        startSSE(token);
        startStatsPolling();
        return true;
      }
    } catch (err) {
      console.error(err);
    }
    return false;
  }

  function updateStats(stats) {
    if (!stats) return;
    totalEmpresas.textContent = (stats.total_empresas || 0).toLocaleString('pt-BR');
    totalEstab.textContent = (stats.total_estabelecimentos || 0).toLocaleString('pt-BR');
    totalSocios.textContent = (stats.total_socios || 0).toLocaleString('pt-BR');
    ultimaCompetencia.textContent = stats.ultima_competencia || 'Pendente';
  }

  function startSSE(token) {
    if (activeSSE) {
      activeSSE.close();
    }
    const sse = new EventSource('/api/v1/events?token=' + encodeURIComponent(token));
    activeSSE = sse;
    sse.onopen = () => {
      appendLog('[Sistema] Conectado ao streaming de logs em tempo real.');
    };
    sse.onmessage = (event) => {
      appendLog(event.data);
    };
    sse.onerror = () => {
      // EventSource reconnects automatically; this just makes the gap
      // visible instead of the console silently going quiet, which used to
      // be indistinguishable from "nothing is happening".
      appendLog('[Sistema] Conexão de logs interrompida. Tentando reconectar...');
    };
  }

  function startStatsPolling() {
    if (statsIntervalId) {
      clearInterval(statsIntervalId);
    }
    statsIntervalId = setInterval(async () => {
      if (!apiToken) return;
      try {
        const res = await fetch('/api/v1/status', {
          headers: { 'X-API-Token': apiToken }
        });
        if (res.ok) {
          const data = await res.json();
          updateStats(data.stats);
        }
      } catch (err) {
        // Silent: the next tick retries, no need to spam the log console.
      }
    }, 15000);
  }

  function appendLog(msg) {
    const line = document.createElement('div');
    line.className = 'log-line';
    line.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
    logConsole.appendChild(line);
    logConsole.scrollTop = logConsole.scrollHeight;
  }

  function showModal() {
    authModal.classList.remove('hidden');
  }

  function hideModal() {
    authModal.classList.add('hidden');
  }
});
