// Copy button handler for the standalone file view page.
// Separate file (not inline) to comply with CSP script-src 'self'.
const btn = document.getElementById("copy-btn");
if (btn) {
  btn.addEventListener("click", async function () {
    try {
      const resp = await fetch(location.pathname + "?raw=1");
      if (!resp.ok) throw new Error("fetch failed: " + resp.status);
      const text = await resp.text();

      // Try modern clipboard API first; falls back to execCommand for
      // non-secure contexts (e.g. http://127.0.0.1 in dev).
      let copied = false;
      if (navigator.clipboard && navigator.clipboard.writeText) {
        try {
          await navigator.clipboard.writeText(text);
          copied = true;
        } catch (clipErr) {
          console.warn("clipboard API failed, falling back to execCommand:", clipErr);
        }
      }
      if (!copied) {
        const ta = document.createElement("textarea");
        ta.value = text;
        ta.style.position = "fixed";
        ta.style.opacity = "0";
        document.body.appendChild(ta);
        ta.select();
        if (!document.execCommand("copy")) {
          throw new Error("execCommand copy returned false");
        }
        document.body.removeChild(ta);
      }

      btn.textContent = "Copied!";
      setTimeout(() => (btn.textContent = "Copy"), 2000);
    } catch (e) {
      console.error("copy failed:", e);
      btn.textContent = "Failed";
      setTimeout(() => (btn.textContent = "Copy"), 2000);
    }
  });
}
