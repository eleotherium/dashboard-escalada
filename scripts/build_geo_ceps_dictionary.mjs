#!/usr/bin/env node
import fs from "node:fs/promises";
import path from "node:path";

const DEFAULT_INPUTS = ["exemplo.md"];
const OUTPUT_JSON = path.resolve("data", "geo_ceps_dictionary.json");
const OUTPUT_CSV = path.resolve("data", "geo_ceps_dictionary.csv");
const OUTPUT_SQL = path.resolve("supabase", "seed_geo_ceps.sql");

const CONCURRENCY = 4;
const REQUEST_RETRIES = 2;
const UF_CODE_TO_NAME = {
  AC: "Acre",
  AL: "Alagoas",
  AP: "Amapa",
  AM: "Amazonas",
  BA: "Bahia",
  CE: "Ceara",
  DF: "Distrito Federal",
  ES: "Espirito Santo",
  GO: "Goias",
  MA: "Maranhao",
  MT: "Mato Grosso",
  MS: "Mato Grosso do Sul",
  MG: "Minas Gerais",
  PA: "Para",
  PB: "Paraiba",
  PR: "Parana",
  PE: "Pernambuco",
  PI: "Piaui",
  RJ: "Rio de Janeiro",
  RN: "Rio Grande do Norte",
  RS: "Rio Grande do Sul",
  RO: "Rondonia",
  RR: "Roraima",
  SC: "Santa Catarina",
  SP: "Sao Paulo",
  SE: "Sergipe",
  TO: "Tocantins",
};

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeText(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toUpperCase();
}

function normalizeCep(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits.length === 8 ? digits : "";
}

function numberOrNull(value) {
  if (value === null || value === undefined) return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function sqlEscape(value) {
  return String(value ?? "").replace(/'/g, "''");
}

async function requestJson(url, opts = {}, retries = REQUEST_RETRIES) {
  let lastError = null;
  for (let attempt = 0; attempt <= retries; attempt += 1) {
    try {
      const response = await fetch(url, opts);
      if (!response.ok) {
        const body = await response.text().catch(() => "");
        const retryable = response.status === 429 || response.status >= 500;
        if (retryable && attempt < retries) {
          await sleep(250 * (attempt + 1));
          continue;
        }
        throw new Error(`HTTP ${response.status} ${url} ${body}`.trim());
      }
      return await response.json();
    } catch (err) {
      lastError = err;
      if (attempt >= retries) break;
      await sleep(250 * (attempt + 1));
    }
  }
  throw lastError;
}

async function fetchBrasilApi(cep) {
  const url = `https://brasilapi.com.br/api/cep/v2/${cep}`;
  try {
    const data = await requestJson(url);
    const coords = data?.location?.coordinates || {};
    return {
      ok: true,
      uf: data?.state || null,
      cidade: data?.city || null,
      bairro: data?.neighborhood || null,
      logradouro: data?.street || null,
      lat: numberOrNull(coords?.latitude),
      lng: numberOrNull(coords?.longitude),
      source: "brasilapi",
    };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

async function fetchViaCep(cep) {
  const url = `https://viacep.com.br/ws/${cep}/json/`;
  try {
    const data = await requestJson(url);
    if (data?.erro) return { ok: false, error: "viacep:not-found" };
    return {
      ok: true,
      uf: data?.uf || null,
      cidade: data?.localidade || null,
      bairro: data?.bairro || null,
      logradouro: data?.logradouro || null,
      source: "viacep",
    };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

const geocodeCache = new Map();
async function geocodeCity(city, ufCode) {
  const cityNorm = String(city || "").trim();
  const ufNorm = String(ufCode || "").trim().toUpperCase();
  if (!cityNorm) return { lat: null, lng: null, source: null };

  const cacheKey = `${normalizeText(cityNorm)}|${ufNorm}`;
  if (geocodeCache.has(cacheKey)) return geocodeCache.get(cacheKey);

  const query = encodeURIComponent(cityNorm);
  const url = `https://geocoding-api.open-meteo.com/v1/search?name=${query}&count=8&language=pt&format=json`;
  try {
    const data = await requestJson(url, {});
    const rows = Array.isArray(data?.results) ? data.results : [];
    const brRows = rows.filter((r) => String(r?.country_code || "").toUpperCase() === "BR");
    const ufNameNorm = normalizeText(UF_CODE_TO_NAME[ufNorm] || ufNorm);
    const exactUf = brRows.find((r) => normalizeText(r?.admin1) === ufNameNorm);
    const chosen = exactUf || brRows[0] || rows[0] || null;
    const result = {
      lat: numberOrNull(chosen?.latitude),
      lng: numberOrNull(chosen?.longitude),
      source: chosen ? "open-meteo" : null,
    };
    geocodeCache.set(cacheKey, result);
    return result;
  } catch {
    const result = { lat: null, lng: null, source: null };
    geocodeCache.set(cacheKey, result);
    return result;
  }
}

function extractCepsFromText(text) {
  const matches = String(text || "").match(/\b\d{8}\b/g) || [];
  const set = new Set();
  for (const m of matches) {
    const cep = normalizeCep(m);
    if (cep) set.add(cep);
  }
  return set;
}

async function loadInputCeps(inputPaths) {
  const all = new Set();
  for (const p of inputPaths) {
    const abs = path.resolve(p);
    try {
      const content = await fs.readFile(abs, "utf8");
      const ceps = extractCepsFromText(content);
      for (const cep of ceps) all.add(cep);
      console.log(`[geo-ceps] input ok: ${p} (${ceps.size} CEPs)`);
    } catch (err) {
      console.warn(`[geo-ceps] input skip: ${p} (${String(err)})`);
    }
  }
  return Array.from(all).sort();
}

async function buildEntry(cep) {
  const br = await fetchBrasilApi(cep);
  const vi = br.ok ? null : await fetchViaCep(cep);

  const uf = (br.ok ? br.uf : vi?.uf) || null;
  const cidade = (br.ok ? br.cidade : vi?.cidade) || null;
  const bairro = (br.ok ? br.bairro : vi?.bairro) || null;
  const logradouro = (br.ok ? br.logradouro : vi?.logradouro) || null;

  let lat = br.ok ? br.lat : null;
  let lng = br.ok ? br.lng : null;
  let geoSource = br.ok && lat !== null && lng !== null ? "brasilapi" : null;

  if ((lat === null || lng === null) && cidade && uf) {
    const geo = await geocodeCity(cidade, uf);
    if (geo.lat !== null && geo.lng !== null) {
      lat = geo.lat;
      lng = geo.lng;
      geoSource = geo.source;
    }
  }

  return {
    cep,
    uf,
    cidade,
    bairro,
    logradouro,
    lat,
    lng,
    city_source: br.ok ? "brasilapi" : (vi?.ok ? "viacep" : null),
    geo_source: geoSource,
    updated_at: new Date().toISOString(),
  };
}

function toCsv(rows) {
  const headers = [
    "cep",
    "uf",
    "cidade",
    "bairro",
    "logradouro",
    "lat",
    "lng",
    "city_source",
    "geo_source",
    "updated_at",
  ];
  const esc = (v) => {
    const s = String(v ?? "");
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((h) => esc(row[h])).join(","));
  }
  return lines.join("\n");
}

function toUpsertSql(rows) {
  const values = rows
    .map((r) => {
      const cep = `'${sqlEscape(r.cep)}'`;
      const lat = r.lat === null ? "null" : Number(r.lat);
      const lng = r.lng === null ? "null" : Number(r.lng);
      const cidade = r.cidade ? `'${sqlEscape(r.cidade)}'` : "null";
      const uf = r.uf ? `'${sqlEscape(r.uf)}'` : "null";
      return `(${cep}, ${lat}, ${lng}, ${cidade}, ${uf}, now())`;
    })
    .join(",\n");

  return [
    "-- auto-generated by scripts/build_geo_ceps_dictionary.mjs",
    "begin;",
    "insert into public.geo_ceps (cep, lat, lng, cidade, uf, updated_at)",
    "values",
    values,
    "on conflict (cep) do update set",
    "  lat = excluded.lat,",
    "  lng = excluded.lng,",
    "  cidade = excluded.cidade,",
    "  uf = excluded.uf,",
    "  updated_at = now();",
    "commit;",
    "",
  ].join("\n");
}

async function main() {
  const inputPaths = process.argv.slice(2);
  const inputs = inputPaths.length ? inputPaths : DEFAULT_INPUTS;
  const ceps = await loadInputCeps(inputs);

  if (!ceps.length) {
    console.error("[geo-ceps] nenhum CEP encontrado nos arquivos de entrada.");
    process.exitCode = 1;
    return;
  }

  console.log(`[geo-ceps] total CEPs unicos: ${ceps.length}`);
  const queue = [...ceps];
  const rows = [];

  async function worker(workerId) {
    while (queue.length) {
      const cep = queue.shift();
      if (!cep) break;
      try {
        const row = await buildEntry(cep);
        rows.push(row);
        console.log(
          `[geo-ceps] [w${workerId}] ${cep} city=${row.cidade || "-"} uf=${row.uf || "-"} lat=${row.lat ?? "null"} lng=${row.lng ?? "null"}`
        );
      } catch (err) {
        rows.push({
          cep,
          uf: null,
          cidade: null,
          bairro: null,
          logradouro: null,
          lat: null,
          lng: null,
          city_source: null,
          geo_source: null,
          updated_at: new Date().toISOString(),
          error: String(err),
        });
        console.warn(`[geo-ceps] [w${workerId}] fail ${cep}: ${String(err)}`);
      }
      await sleep(100);
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(CONCURRENCY, ceps.length) }, (_, i) => worker(i + 1))
  );

  rows.sort((a, b) => String(a.cep).localeCompare(String(b.cep)));

  await fs.mkdir(path.dirname(OUTPUT_JSON), { recursive: true });
  await fs.mkdir(path.dirname(OUTPUT_SQL), { recursive: true });
  await fs.writeFile(OUTPUT_JSON, `${JSON.stringify(rows, null, 2)}\n`, "utf8");
  await fs.writeFile(OUTPUT_CSV, `${toCsv(rows)}\n`, "utf8");
  await fs.writeFile(OUTPUT_SQL, toUpsertSql(rows), "utf8");

  const withCity = rows.filter((r) => r.cidade).length;
  const withGeo = rows.filter((r) => r.lat !== null && r.lng !== null).length;
  console.log(`[geo-ceps] json: ${OUTPUT_JSON}`);
  console.log(`[geo-ceps] csv: ${OUTPUT_CSV}`);
  console.log(`[geo-ceps] sql: ${OUTPUT_SQL}`);
  console.log(`[geo-ceps] cobertura cidade: ${withCity}/${rows.length}`);
  console.log(`[geo-ceps] cobertura lat/lng: ${withGeo}/${rows.length}`);
}

main().catch((err) => {
  console.error("[geo-ceps] fatal:", err);
  process.exitCode = 1;
});
