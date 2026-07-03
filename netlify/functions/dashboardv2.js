// Proxy servidor para as RPCs do Supabase.
// O browser so enxerga /api/dashboardv2 - a URL e a anon key do Supabase
// ficam somente nas env vars do Netlify (SUPABASE_URL / SUPABASE_ANON_KEY).

const MAX_LIMIT = 200000;

const REPORTS = {
  main: {
    rpcName: process.env.DASHBOARD_RPC_NAME || "dashboard_executivo_v2_interno",
    allowedParams: ["p_date_from", "p_date_to", "p_uf", "p_publico", "p_limit"],
  },
  sources: {
    rpcName: process.env.DASHBOARD_SOURCES_RPC_NAME || "dashboard_fontes_v2_interno",
    allowedParams: ["p_date_from", "p_date_to", "p_uf", "p_publico", "p_limit"],
  },
  comunidade: {
    rpcName: "comunidade_metricas_v2",
    allowedParams: ["p_date_from", "p_date_to"],
  },
};

function buildRpcParams(report, query) {
  const params = {};
  for (const key of report.allowedParams) {
    const raw = query[key];
    if (raw === undefined || raw === null || raw === "") {
      params[key] = null;
      continue;
    }
    if (key === "p_limit") {
      const n = Number(raw);
      params[key] = Number.isFinite(n) && n > 0 ? Math.min(n, MAX_LIMIT) : MAX_LIMIT;
      continue;
    }
    params[key] = String(raw);
  }
  return params;
}

exports.handler = async (event) => {
  if (event.httpMethod !== "GET") {
    return { statusCode: 405, body: JSON.stringify({ error: "Method not allowed." }) };
  }

  const SUPABASE_URL = process.env.SUPABASE_URL;
  const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY;
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Supabase nao configurado no servidor (SUPABASE_URL/SUPABASE_ANON_KEY)." }),
    };
  }

  const query = event.queryStringParameters || {};
  const reportKey = REPORTS[query.report] ? query.report : "main";
  const report = REPORTS[reportKey];
  const params = buildRpcParams(report, query);

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 15000);

  try {
    const upstream = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${report.rpcName}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: SUPABASE_ANON_KEY,
        Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
      },
      body: JSON.stringify(params),
      signal: controller.signal,
    });

    const text = await upstream.text();
    if (!upstream.ok) {
      return {
        statusCode: upstream.status,
        headers: { "Content-Type": "application/json" },
        body: text || JSON.stringify({ error: "Falha ao consultar dados." }),
      };
    }

    return {
      statusCode: 200,
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
      body: text,
    };
  } catch (err) {
    const timedOut = err && err.name === "AbortError";
    return {
      statusCode: timedOut ? 504 : 502,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ error: timedOut ? "Timeout ao consultar upstream." : "Falha ao contatar upstream." }),
    };
  } finally {
    clearTimeout(timeout);
  }
};
