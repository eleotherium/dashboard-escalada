const { chromium } = require("playwright");

async function text(page, selector) {
  const locator = page.locator(selector);
  return (await locator.textContent())?.trim() || "";
}

async function snapshot(page, family) {
  return {
    family,
    url: page.url(),
    title: await text(page, "#sourcesSectionTitle"),
    subtitle: await text(page, "#sourcesSectionSubtitle"),
    total: await text(page, "#sourcesTotal"),
    inscritos: await text(page, "#sourcesInscritosTotal"),
    atendimentos: await text(page, "#sourcesAtendTotal"),
    coverage: await text(page, "#sourcesCoverageHint"),
    firstRow: await page.locator("#sourcesTopList button").first().textContent().catch(() => ""),
  };
}

async function run() {
  console.log("launching browser");
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 } });
  page.on("console", (msg) => console.log(`[console:${msg.type()}] ${msg.text()}`));
  page.on("pageerror", (err) => console.log(`[pageerror] ${err.message}`));
  page.on("requestfailed", (request) => {
    console.log(`[requestfailed] ${request.method()} ${request.url()} ${request.failure()?.errorText || ""}`);
  });

  console.log("goto start");
  await page.goto("http://127.0.0.1:4173/index.html#sources", {
    waitUntil: "domcontentloaded",
    timeout: 120000,
  });
  console.log("goto ok");

  await page.waitForSelector('[data-page="sources"]:not(.hidden)', { timeout: 120000 });
  console.log("sources page visible");
  await page.waitForTimeout(2500);

  const before = await snapshot(page, "geral");
  console.log("snapshot geral", JSON.stringify(before));

  const directCall = await page.evaluate(() => {
    const fn = window.__setSourceFamily;
    if (typeof fn !== "function") return { hasFn: false, href: window.location.href };
    fn("eventos");
    return { hasFn: true, href: window.location.href };
  });
  console.log("direct call", JSON.stringify(directCall));
  await page.waitForTimeout(1000);
  const afterDirect = await snapshot(page, "eventos-direct");
  console.log("snapshot eventos-direct", JSON.stringify(afterDirect));

  const families = ["eventos", "multiplicadores", "embaixadores", "convites", "geral"];
  const results = [before, afterDirect];

  for (const family of families) {
    console.log(`clicking ${family}`);
    const clickResult = await page.evaluate((targetFamily) => {
      const btn = document.querySelector(`[data-source-family="${targetFamily}"]`);
      if (!btn) return { found: false };
      const event = new MouseEvent("click", { bubbles: true, cancelable: true });
      const notCanceled = btn.dispatchEvent(event);
      return {
        found: true,
        notCanceled,
        defaultPrevented: event.defaultPrevented,
        href: btn.getAttribute("href"),
        locationHref: window.location.href,
      };
    }, family);
    console.log(`click result ${family}`, JSON.stringify(clickResult));
    await page.waitForTimeout(1000);
    const snap = await snapshot(page, family);
    console.log(`snapshot ${family}`, JSON.stringify(snap));
    results.push(snap);
  }

  console.log(JSON.stringify(results, null, 2));
  await browser.close();
}

run().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
