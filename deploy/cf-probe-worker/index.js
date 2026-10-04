export default {
  async fetch(request, env) {
    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    // Reject all requests when secret is not configured
    if (!env.PROBE_SECRET) {
      return new Response("Unauthorized", { status: 401 });
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
    const rawBytes  = body.max_bytes;
    const maxBytes  = (Number.isFinite(rawBytes) && rawBytes > 0)
      ? Math.min(rawBytes, 2 * 1024 * 1024)   // cap at 2 MB
      : 65536;

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

    // Redirect hop cap comes from the caller (config.py CF_WORKER_MAX_HOPS); the default only
    // applies to older callers that do not send max_hops.
    const rawHops = body.max_hops;
    const maxHops = (Number.isFinite(rawHops) && rawHops > 0)
      ? Math.min(Math.floor(rawHops), 20)
      : 8;

    try {
      // Redirects are followed manually so Set-Cookie names can be collected from EVERY hop
      // (parking vendors set their marker cookie on an intermediate redirect) — the same
      // accumulation the direct tier does in jobs/public_domain.py::_fetch_chain.
      const cookies = [];
      let current = targetUrl;
      let resp = null;
      for (let hop = 0; hop <= maxHops; hop++) {
        resp = await fetch(current, {
          headers: {
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
          },
          redirect: "manual",
        });
        for (const sc of (resp.headers.getSetCookie ? resp.headers.getSetCookie() : [])) {
          const name = sc.split("=")[0].trim();
          if (name && !cookies.includes(name)) cookies.push(name);
        }
        const loc = [301, 302, 303, 307, 308].includes(resp.status) ? resp.headers.get("Location") : null;
        if (!loc) break;
        let next;
        try {
          next = new URL(loc, current);
        } catch {
          return Response.json({ status: 0, final_url: null, body: null, error: "Bad redirect Location" });
        }
        if (!["http:", "https:"].includes(next.protocol)) {
          return Response.json({ status: 0, final_url: null, body: null, error: "Redirect to non-http(s)" });
        }
        if (hop === maxHops) {
          return Response.json({ status: 0, final_url: null, body: null, error: "hop_limit" });
        }
        if (resp.body) resp.body.cancel().catch(() => {});
        current = next.toString();
      }

      // Read incrementally up to maxBytes — avoids buffering huge responses.
      // resp.body may be null for 204 No Content or HEAD responses.
      let text = "";
      let truncated = false;
      if (resp.body) {
        const reader = resp.body.getReader();
        const chunks = [];
        let received = 0;
        while (received < maxBytes) {
          const { done, value } = await reader.read();
          if (done) break;
          const slice = value.slice(0, maxBytes - received);
          chunks.push(slice);
          received += slice.byteLength;
        }
        // Body bigger than maxBytes? Peek one more read so the caller can tell "exactly
        // maxBytes" from "cut off" (an oversize page is a real site, never a parked stub).
        if (received >= maxBytes) {
          const { done } = await reader.read();
          truncated = !done;
        }
        reader.cancel().catch(() => {});
        const buf = new Uint8Array(received);
        let pos = 0;
        for (const c of chunks) { buf.set(c, pos); pos += c.byteLength; }
        text = new TextDecoder("utf-8", { fatal: false }).decode(buf);
      }

      const headers = {};
      resp.headers.forEach((v, k) => { headers[k] = v; });

      return Response.json({
        status:    resp.status,
        final_url: current,
        body:      text,
        truncated: truncated,
        headers:   headers,
        cookies:   cookies,
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
