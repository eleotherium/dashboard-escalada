// Arquivo de configuracao do dashboard no browser (sem require/import).
// Os dados agora sao buscados via Netlify Function (/api/dashboardv2), que
// guarda SUPABASE_URL/SUPABASE_ANON_KEY como env vars no servidor. O browser
// nunca ve o Supabase diretamente.
window.__ENV = {
  // Endpoint proprio (Netlify Function) que faz a ponte com o Supabase.
  DASHBOARD_API_BASE: "/api/dashboardv2",

  // "internal" (login obrigatorio via Supabase Auth) ou "public".
  // No modo "internal", o client-side ainda precisa de SUPABASE_URL/ANON_KEY
  // (exclusivamente para o fluxo de OAuth) - preencha-os aqui se ativar esse modo.
  DASHBOARD_AUTH_MODE: "public",
  // Provedor OAuth para login no gate de autenticacao
  SUPABASE_OAUTH_PROVIDER: "google",

  // Limite maximo de registros retornados pela RPC
  RPC_LIMIT: 200000,
  // Timeout da chamada RPC (ms)
  RPC_TIMEOUT_MS: 12000,

  // Em dev, pode ser true para cair no mock quando a API falhar/nao configurar
  USE_MOCK_FALLBACK: true,
};
