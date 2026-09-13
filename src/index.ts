export interface Env {
  DB: D1Database;

  NIM_API_KEY: string;
  NIM_BASE_URL?: string;
  NIM_MODEL: string;

  TELEGRAM_BOT_TOKEN: string;
  TELEGRAM_CHAT_ID: string;
}

interface Article {
  title: string;
  url: string;
  source: string;
  category: string;
  publishedAt: string;
  description: string;
}

interface AISelection {
  selected: boolean;
  importance: number;
  category: string;
  reason: string;
}

interface AIEvent {
  title: string;
  category: string;
  importance: number;
  summary: string;
  why_it_matters: string;
  market_impact: string;
  countries: string[];
  companies: string[];
  assets: string[];
  source_urls: string[];
}

const RSS_SOURCES = [
  { name: "OpenAI", url: "https://openai.com/news/rss.xml", category: "AI" },
  { name: "Google AI", url: "https://blog.google/technology/ai/rss/", category: "AI" },
  { name: "Google DeepMind", url: "https://deepmind.google/blog/rss.xml", category: "AI" },
  { name: "NVIDIA", url: "https://nvidianews.nvidia.com/rss", category: "AI" },
  { name: "Microsoft AI", url: "https://blogs.microsoft.com/ai/feed/", category: "AI" },
  { name: "TechCrunch", url: "https://techcrunch.com/category/artificial-intelligence/feed/", category: "AI" },
  { name: "MIT Technology Review", url: "https://www.technologyreview.com/feed/", category: "AI" },
  { name: "Federal Reserve", url: "https://www.federalreserve.gov/feeds/press_all.xml", category: "宏观金融" },
  { name: "ECB", url: "https://www.ecb.europa.eu/rss/press.html", category: "宏观金融" },
  { name: "IMF", url: "https://www.imf.org/en/News/RSS", category: "宏观经济" },
  { name: "World Bank", url: "https://www.worldbank.org/en/news/all?format=rss", category: "宏观经济" },
  { name: "CoinDesk", url: "https://www.coindesk.com/arc/outboundfeeds/rss/", category: "Crypto" },
  { name: "Cointelegraph", url: "https://cointelegraph.com/rss", category: "Crypto" },
  { name: "Decrypt", url: "https://decrypt.co/feed", category: "Crypto" }
];

const GDELT_QUERIES = [
  {
    query: '("artificial intelligence" OR AI OR OpenAI OR Anthropic OR Nvidia)',
    category: "AI"
  },
  {
    query: '(Federal Reserve OR ECB OR interest rates OR inflation OR Treasury)',
    category: "宏观金融"
  },
  {
    query: '(Bitcoin OR Ethereum OR cryptocurrency OR stablecoin OR crypto ETF)',
    category: "Crypto"
  },
  {
    query: '(China OR United States) (trade OR tariff OR export controls OR semiconductor)',
    category: "国际经贸"
  }
];

export default {
  async fetch(
    request: Request,
    env: Env,
    ctx: ExecutionContext
  ): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return json({
        ok: true,
        service: "AI Global News Intelligence",
        version: "1.0.0"
      });
    }

    if (url.pathname === "/health") {
      return json({ ok: true, time: new Date().toISOString() });
    }

    if (url.pathname === "/run") {
      if (url.searchParams.get("key") !== env.NIM_API_KEY.slice(0, 8)) {
        return new Response("Unauthorized", { status: 401 });
      }

      ctx.waitUntil(runDailyDigest(env));

      return json({ ok: true, message: "News pipeline started" });
    }

    return new Response("Not Found", { status: 404 });
  },

  async scheduled(
    controller: ScheduledController,
    env: Env,
    ctx: ExecutionContext
  ): Promise<void> {
    console.log(`Cron started: ${controller.cron} ${controller.scheduledTime}`);

    ctx.waitUntil(runDailyDigest(env));
  }
};

/* =========================================================
   MAIN PIPELINE
========================================================= */

async function runDailyDigest(env: Env): Promise<void> {
  const startedAt = Date.now();

  console.log("Starting AI Global News pipeline");

  try {
    await ensureSources(env);

    const articles = await collectNews();

    console.log(`Collected ${articles.length} raw articles`);

    const filtered = await filterAndDeduplicate(env, articles);

    console.log(`After filtering: ${filtered.length}`);

    if (filtered.length === 0) {
      await sendTelegram(
        env,
        "🌍 全球 AI & 金融情报\n\n过去24小时没有发现达到推送标准的重要新闻。"
      );

      return;
    }

    const candidates = await aiSelectArticles(env, filtered);

    console.log(`AI selected ${candidates.length} articles`);

    if (candidates.length === 0) {
      await sendTelegram(
        env,
        "🌍 全球 AI & 金融情报\n\n今日没有发现足够重要的新闻。"
      );

      return;
    }

    const events = await aiBuildEvents(env, candidates);

    console.log(`AI created ${events.length} events`);

    const importantEvents = events
      .filter((e) => e.importance >= 60)
      .sort((a, b) => b.importance - a.importance)
      .slice(0, 15);

    const digest = await aiBuildDigest(env, importantEvents);

    await saveEvents(env, importantEvents);

    await saveDigest(env, digest, importantEvents.length);

    await sendTelegram(env, digest);

    console.log(`Pipeline completed in ${Date.now() - startedAt}ms`);
  } catch (error) {
    console.error("Pipeline failed", error);

    const message = error instanceof Error ? error.message : String(error);

    try {
      await sendTelegram(
        env,
        `⚠️ AI全球情报机器人执行失败\n\n${message.slice(0, 1000)}`
      );
    } catch (telegramError) {
      console.error("Failed to send error notification", telegramError);
    }

    throw error;
  }
}

/* =========================================================
   RSS
========================================================= */

async function fetchRSS(
  source: (typeof RSS_SOURCES)[number]
): Promise<Article[]> {
  try {
    const response = await fetch(source.url, {
      headers: {
        "User-Agent": "AI-Global-News-Worker/1.0 (+Cloudflare Worker)"
      }
    });

    if (!response.ok) {
      console.error(`RSS failed ${source.name}: ${response.status}`);
      return [];
    }

    const xml = await response.text();

    return parseRSS(xml, source);
  } catch (error) {
    console.error(`RSS exception ${source.name}`, error);
    return [];
  }
}

function parseRSS(
  xml: string,
  source: (typeof RSS_SOURCES)[number]
): Article[] {
  const articles: Article[] = [];

  const itemMatches = xml.match(/<item[\s\S]*?<\/item>/gi) || [];

  for (const item of itemMatches.slice(0, 30)) {
    const title = cleanText(getXmlValue(item, "title"));

    const link =
      cleanText(getXmlValue(item, "link")) ||
      cleanText(getXmlValue(item, "guid"));

    const description = cleanText(getXmlValue(item, "description"));

    const published =
      getXmlValue(item, "pubDate") ||
      getXmlValue(item, "published") ||
      getXmlValue(item, "updated");

    if (!title || !link) {
      continue;
    }

    const publishedAt = parseDate(published);

    if (!publishedAt) {
      continue;
    }

    if (Date.now() - new Date(publishedAt).getTime() > 36 * 60 * 60 * 1000) {
      continue;
    }

    articles.push({
      title,
      url: link,
      source: source.name,
      category: source.category,
      publishedAt,
      description: description.slice(0, 1000)
    });
  }

  return articles;
}

/* =========================================================
   GDELT
========================================================= */

async function fetchGDELT(query: string, category: string): Promise<Article[]> {
  try {
    const url =
      "https://api.gdeltproject.org/api/v2/doc/doc?" +
      new URLSearchParams({
        query,
        mode: "artlist",
        maxrecords: "50",
        timespan: "24h",
        sort: "datedesc",
        format: "json"
      });

    const response = await fetch(url);

    if (!response.ok) {
      console.error(`GDELT failed: ${response.status}`);
      return [];
    }

    const data = await response.json<any>();

    const articles: Article[] = [];

    for (const item of data.articles || []) {
      if (!item.title || !item.url) {
        continue;
      }

      articles.push({
        title: cleanText(item.title),
        url: item.url,
        source: item.domain || "GDELT",
        category,
        publishedAt: item.seendate
          ? parseGDELTDate(item.seendate)
          : new Date().toISOString(),
        description: cleanText(item.snippet || item.title).slice(0, 1000)
      });
    }

    return articles;
  } catch (error) {
    console.error("GDELT exception", error);
    return [];
  }
}

/* =========================================================
   COLLECT
========================================================= */

async function collectNews(): Promise<Article[]> {
  const rssResults = await Promise.all(RSS_SOURCES.map(fetchRSS));

  const gdeltResults = await Promise.all(
    GDELT_QUERIES.map((item) => fetchGDELT(item.query, item.category))
  );

  const all = [...rssResults.flat(), ...gdeltResults.flat()];

  const map = new Map<string, Article>();

  for (const article of all) {
    const normalized = normalizeUrl(article.url);

    if (!map.has(normalized)) {
      map.set(normalized, article);
    }
  }

  return Array.from(map.values());
}

/* =========================================================
   FILTER + DEDUP
========================================================= */

async function filterAndDeduplicate(
  env: Env,
  articles: Article[]
): Promise<Article[]> {
  const result: Article[] = [];

  for (const article of articles) {
    const hash = await sha256(
      `${article.title.toLowerCase()}|${normalizeUrl(article.url)}`
    );

    const existing = await env.DB.prepare(
      `
      SELECT id
      FROM articles
      WHERE content_hash = ?
      LIMIT 1
      `
    )
      .bind(hash)
      .first();

    if (existing) {
      continue;
    }

    await env.DB.prepare(
      `
      INSERT OR IGNORE INTO articles
      (
        title,
        url,
        description,
        published_at,
        category,
        content_hash
      )
      VALUES (?, ?, ?, ?, ?, ?)
      `
    )
      .bind(
        article.title,
        article.url,
        article.description,
        article.publishedAt,
        article.category,
        hash
      )
      .run();

    result.push(article);
  }

  return result;
}

/* =========================================================
   AI SELECT
========================================================= */

async function aiSelectArticles(
  env: Env,
  articles: Article[]
): Promise<Article[]> {
  const chunks = chunk(articles, 40);

  const selected: Article[] = [];

  for (const batch of chunks) {
    const prompt = `
你是一个全球新闻情报分析员。

你的任务不是寻找普通新闻，而是寻找真正值得一个长期关注 AI、金融、Crypto、宏观经济和国际经贸的人知道的重大事件。

判断标准：

10 = 全球重大事件
9 = 全球金融/科技/贸易重大事件
8 = 行业重大事件
7 = 明显值得关注
6 = 一般重要
5以下 = 不值得进入日报

重点关注：

AI
人工智能
半导体
NVIDIA
OpenAI
Anthropic
Google
Microsoft
宏观经济
美联储
ECB
中国经济
美国经济
利率
通胀
美元
国债
股市
Bitcoin
Ethereum
Stablecoin
Crypto ETF
监管
中美贸易
关税
出口管制
能源
地缘政治

忽略：

普通产品更新
普通融资新闻
营销新闻
无关娱乐
低价值评论
重复报道
没有实质信息的文章

下面是候选新闻：

${JSON.stringify(
  batch.map((a, index) => ({
    id: index,
    title: a.title,
    source: a.source,
    category: a.category,
    publishedAt: a.publishedAt,
    description: a.description
  }))
)}

只返回 JSON 数组：

[
  {
    "id": 0,
    "selected": true,
    "importance": 85,
    "category": "AI",
    "reason": "一句话说明为什么重要"
  }
]

只保留 selected=true 的新闻。
`;

    const response = await callK3(env, prompt);

    const parsed = safeJsonArray(response);

    for (const item of parsed) {
      const index = Number(item.id);

      if (
        !Number.isInteger(index) ||
        index < 0 ||
        index >= batch.length
      ) {
        continue;
      }

      if (
        item.selected === true &&
        Number(item.importance) >= 60
      ) {
        selected.push(batch[index]);
      }
    }
  }

  return selected.slice(0, 50);
}

/* =========================================================
   AI EVENT CLUSTERING
========================================================= */

async function aiBuildEvents(
  env: Env,
  articles: Article[]
): Promise<AIEvent[]> {
  const prompt = `
你是全球宏观、AI、金融、Crypto和国际经贸情报分析员。

下面是一批新闻。

你的任务：

1. 把报道同一个事件的新闻合并。
2. 不要把同一个事件重复输出。
3. 对每个独立事件进行中文分析。
4. 不要编造新闻中不存在的事实。
5. 如果无法确认市场影响，就明确写“暂无明确市场影响”。
6. importance必须是0-100。
7. 重点识别真正可能改变市场、政策、科技产业或国际关系的事件。

输出 JSON 数组：

[
  {
    "title": "简洁中文事件标题",
    "category": "AI/金融/宏观经济/Crypto/国际经贸/地缘政治/半导体/其他",
    "importance": 90,
    "summary": "发生了什么",
    "why_it_matters": "为什么值得关注",
    "market_impact": "可能影响哪些市场、行业或资产",
    "countries": ["美国"],
    "companies": ["NVIDIA"],
    "assets": ["NASDAQ"],
    "source_urls": ["https://..."]
  }
]

新闻：

${JSON.stringify(
  articles.map((a) => ({
    title: a.title,
    url: a.url,
    source: a.source,
    category: a.category,
    publishedAt: a.publishedAt,
    description: a.description
  }))
)}
`;

  const response = await callK3(env, prompt);

  return safeJsonArray(response)
    .map((item) => ({
      title:
String(item.title || ""),
      category: String(item.category || "其他"),
      importance: clamp(
        Number(item.importance || 0),
        0,
        100
      ),
      summary: String(item.summary || ""),
      why_it_matters: String(
        item.why_it_matters || ""
      ),
      market_impact: String(
        item.market_impact || ""
      ),
      countries: Array.isArray(item.countries)
        ? item.countries.map(String)
        : [],
      companies: Array.isArray(item.companies)
        ? item.companies.map(String)
        : [],
      assets: Array.isArray(item.assets)
        ? item.assets.map(String)
        : [],
      source_urls: Array.isArray(item.source_urls)
        ? item.source_urls.map(String)
        : []
    }))
    .filter((e) => e.title);
}

/* =========================================================
   AI DAILY DIGEST
========================================================= */

async function aiBuildDigest(
  env: Env,
  events: AIEvent[]
): Promise<string> {
  const today = new Date().toISOString().slice(0, 10);

  const prompt = `
你是一个高质量全球情报日报编辑。

请根据下面的重大事件，制作一份中文 Telegram 日报。

日期：
${today}

要求：

1. 最重要的事件放前面。
2. 不要重复。
3. 不要夸张。
3. 不要编造。
4. 不要做具体买卖建议。
5. 可以分析潜在影响，但必须使用“可能”“值得关注”等谨慎表达。
6. 让普通人能够快速读懂。
7. 突出 AI、金融、Crypto、宏观经济、国际经贸。
8. 总长度控制在约3000-5000中文字符。
9. 每条新闻保留来源链接。

格式：

🌍 全球 AI & 金融情报
${today}

━━━━━━━━━━━━

🔴 今日最重要

1️⃣ 标题
重要性：★★★★★

发生了什么：
...

为什么重要：
...

潜在影响：
...

来源：
链接

━━━━━━━━━━━━

🤖 AI

...

━━━━━━━━━━━━

💰 金融 / 宏观

...

━━━━━━━━━━━━

₿ Crypto

...

━━━━━━━━━━━━

🌐 国际经贸 / 地缘

...

━━━━━━━━━━━━

📊 今日观察

AI：
金融：
Crypto：
国际经贸：

最后不要添加投资建议。

事件：

${JSON.stringify(events)}
`;

  return callK3(env, prompt);
}

/* =========================================================
   K3 API
========================================================= */

async function callK3(
  env: Env,
  prompt: string
): Promise<string> {
  const base =
    env.NIM_BASE_URL ||
    "https://integrate.api.nvidia.com/v1";

  const response = await fetch(
    `${base.replace(/\/$/, "")}/chat/completions`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${env.NIM_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: env.NIM_MODEL,

        messages: [
          {
            role: "system",
            content:
              "你是严谨的全球新闻与金融情报分析模型。必须忠于输入事实，不得编造。"
          },
          {
            role: "user",
            content: prompt
          }
        ],

        temperature: 0.2,

        max_tokens: 6000,

        stream: false
      })
    }
  );

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `NIM API ${response.status}: ${text.slice(0, 2000)}`
    );
  }

  const data = JSON.parse(text);

  const content =
    data?.choices?.[0]?.message?.content;

  if (!content) {
    throw new Error(
      "NIM API returned no message content"
    );
  }

  return String(content);
}

/* =========================================================
   TELEGRAM
========================================================= */

async function sendTelegram(
  env: Env,
  message: string
): Promise<void> {
  const chunks = splitTelegramMessage(message);

  for (const chunk of chunks) {
    const response = await fetch(
      `https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          chat_id: env.TELEGRAM_CHAT_ID,
          text: chunk,
          disable_web_page_preview: true
        })
      }
    );

    const text = await response.text();

    if (!response.ok) {
      throw new Error(
        `Telegram API ${response.status}: ${text}`
      );
    }
  }
}

function splitTelegramMessage(
  message: string
): string[] {
  const MAX = 3900;

  if (message.length <= MAX) {
    return [message];
  }

  const chunks: string[] = [];

  let current = "";

  for (const line of message.split("\n")) {
    if (
      current.length + line.length + 1
>
      MAX
    ) {
      chunks.push(current);
      current = "";
    }

    current += `${line}\n`;
  }

  if (current) {
    chunks.push(current);
  }

  return chunks;
}

/* =========================================================
   D1
========================================================= */

async function ensureSources(
  env: Env
): Promise<void> {
  for (const source of RSS_SOURCES) {
    await env.DB.prepare(
      `
      INSERT OR IGNORE INTO sources
      (name, url, category, type)
      VALUES (?, ?, ?, 'rss')
      `
    )
      .bind(
        source.name,
        source.url,
        source.category
      )
      .run();
  }
}

async function saveEvents(
  env: Env,
  events: AIEvent[]
): Promise<void> {
  for (const event of events) {
    const eventKey = await sha256(
      `${event.title}|${new Date()
        .toISOString()
        .slice(0, 10)}`
    );

    await env.DB.prepare(
      `
      INSERT OR REPLACE INTO events
      (
        event_key,
        title,
        category,
        importance,
        summary,
        why_it_matters,
        market_impact,
        countries,
        companies,
        assets,
        source_urls
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    )
      .bind(
        eventKey,
        event.title,
        event.category,
        event.importance,
        event.summary,
        event.why_it_matters,
        event.market_impact,
        JSON.stringify(event.countries),
        JSON.stringify(event.companies),
        JSON.stringify(event.assets),
        JSON.stringify(event.source_urls)
      )
      .run();
  }
}

async function saveDigest(
  env: Env,
  content: string,
  eventCount: number
): Promise<void> {
  const date = new Date()
    .toISOString()
    .slice(0, 10);

  await env.DB.prepare(
    `
    INSERT OR REPLACE INTO daily_digest
    (digest_date, content, event_count)
    VALUES (?, ?, ?)
    `
  )
    .bind(date, content, eventCount)
    .run();
}

/* =========================================================
   UTILITIES
========================================================= */

function getXmlValue(
  xml: string,
  tag: string
): string {
  const regex = new RegExp(
    `<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)<\\/${tag}>`,
    "i"
  );

  const match = xml.match(regex);

  return match?.[1] || "";
}

function cleanText(text: string): string {
  return decodeHtml(
    text
      .replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, "$1")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim()
  );
}

function decodeHtml(text: string): string {
  return text
    .replace(/&/g, "&")
    .replace(/</g, "<")
    .replace(/>/g, ">")
    .replace(/"/g, '"')
    .replace(/'/g, "'")
    .replace(/'/gi, "'")
    .replace(/&#x2F;/gi, "/");
}

function parseDate(
  value: string
): string | null {
  if (!value) {
    return null;
  }

  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return date.toISOString();
}

function parseGDELTDate(
  value: string
): string {
  const match = value.match(
    /^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})/
  );

  if (!match) {
    return new Date().toISOString();
  }

  const [
    ,
    year,
    month,
    day,
    hour,
    minute,
    second
  ] = match;

  return new Date(
    Date.UTC(
      Number(year),
      Number(month) - 1,
      Number(day),
      Number(hour),
      Number(minute),
      Number(second)
    )
  ).toISOString();
}

function normalizeUrl(url: string): string {
  try {
    const u = new URL(url);

    u.hash = "";

    [
      "utm_source",
      "utm_medium",
      "utm_campaign",
      "utm_term",
      "utm_content",
      "fbclid",
      "gclid"
    ].forEach((key) => {
      u.searchParams.delete(key);
    });

    return u.toString();
  } catch {
    return url;
  }
}

async function sha256(
  input: string
): Promise<string> {
  const data = new TextEncoder().encode(input);

  const hash = await crypto.subtle.digest(
    "SHA-256",
    data
  );

  return Array.from(
    new Uint8Array(hash)
)
    .map((b) =>
      b.toString(16).padStart(2, "0")
    )
    .join("");
}

function chunk<T>(
  array: T[],
  size: number
): T[][] {
  const result: T[][] = [];

  for (let i = 0; i < array.length; i += size) {
    result.push(array.slice(i, i + size));
  }

  return result;
}

function clamp(
  value: number,
  min: number,
  max: number
): number {
  return Math.max(
    min,
    Math.min(max, value)
  );
}

function safeJsonArray(
  text: string
): any[] {
  try {
    const cleaned = text
      .replace(/^```json\s*/i, "")
      .replace(/^```\s*/i, "")
      .replace(/```\s*$/i, "")
      .trim();

    const start = cleaned.indexOf("[");
    const end = cleaned.lastIndexOf("]");

    if (start === -1 || end === -1) {
      return [];
    }

    const json = cleaned.slice(
      start,
      end + 1
    );

    const parsed = JSON.parse(json);

    return Array.isArray(parsed)
      ? parsed
      : [];
  } catch (error) {
    console.error(
      "JSON parse failed",
      error,
      text.slice(0, 1000)
    );

    return [];
  }
}

function json(
  data: unknown,
  status = 200
): Response {
  return new Response(
    JSON.stringify(data, null, 2),
    {
      status,
      headers: {
        "Content-Type": "application/json; charset=utf-8"
      }
    }
  );
}
