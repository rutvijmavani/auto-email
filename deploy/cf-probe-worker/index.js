export default {
  async fetch(request, env) {
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    const auth = request.headers.get("Authorization") || "";
    if (auth !== `Bearer ${env.PROBE_SECRET}`) {
      return new Response("Unauthorized", { status: 401 });
    }

    let body;
    try {
      body = await request.json();
    } catch {
      return Response.json({ error: "Invalid JSON body" }, { status: 400 });
    }

    const targetUrl = body.url;
    const maxBytes  = body.max_bytes || 65536;

    if (!targetUrl || typeof targetUrl !== "string") {
      return Response.json({ error: "Missing url" }, { status: 400 });
    }

    // SSRF guard: only http/https
    let parsed;
    try {
      parsed = new URL(targetUrl);
    } catch {
      return Response.json({ error: "Invalid URL" }, { status: 400 });
    }
    if (!["http:", "https:"].includes(parsed.protocol)) {
      return Response.json({ error: "Only http/https URLs allowed" }, { status: 400 });
    }

    try {
      const resp = await fetch(targetUrl, {
        headers: {
          "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "en-US,en;q=0.5",
        },
        redirect: "follow",
      });

      const buf  = await resp.arrayBuffer();
      const text = new TextDecoder("utf-8", { fatal: false }).decode(buf.slice(0, maxBytes));

      return Response.json({
        status:    resp.status,
        final_url: resp.url,
        body:      text,
        error:     null,
      });
    } catch (e) {
      return Response.json({
        status:    0,
        final_url: null,
        body:      null,
        error:     String(e),
      });
    }
  },
};
