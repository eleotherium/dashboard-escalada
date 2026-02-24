// Arquivo de configuracao do dashboard no browser (sem require/import).
// Preencha com suas credenciais do Supabase para modo "internal" em producao.
window.__ENV = {
  SUPABASE_URL: "https://database.tnledu.shop",
  SUPABASE_ANON_KEY: "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.ewogICJyb2xlIjogImFub24iLAogICJpc3MiOiAic3VwYWJhc2UiLAogICJpYXQiOiAxNzE1MDUwODAwLAogICJleHAiOiAxODcyODE3MjAwCn0.FRcy1kxU_0vQ4JBPnkcxZPfc_RFOFKMGkj00MvBRmKM",

  // "internal" (recomendado) ou "public"
  DASHBOARD_AUTH_MODE: "public",
  // Provedor OAuth para login no gate de autenticacao
  SUPABASE_OAUTH_PROVIDER: "google",

  // Nome da RPC no Supabase
  DASHBOARD_RPC_NAME: "dashboard_executivo_v1",

  // Limite maximo de registros retornados pela RPC
  RPC_LIMIT: 200000,
  // Timeout da chamada RPC (ms)
  RPC_TIMEOUT_MS: 12000,

  // Em dev, pode ser true para cair no mock quando RPC falhar/nao configurar
  USE_MOCK_FALLBACK: true,
};
