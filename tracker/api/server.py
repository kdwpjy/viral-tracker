"""
경량 HTTP 서버 — 외부 라이브러리 없이 표준 라이브러리만 사용
issues.json을 읽어서 REST API로 제공 + index.html 서빙

실행: python -m tracker.api.server
"""
import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

DATA_FILE   = Path(__file__).parent.parent / "data" / "issues.json"
STATIC_ROOT = Path(__file__).parent.parent.parent  # index.html 위치


def load_data() -> dict:
    if not DATA_FILE.exists():
        return {"hot": [], "feed": [], "rising": [], "timeline": [], "versus": {}, "updated_at": ""}
    with open(DATA_FILE, encoding="utf-8") as f:
        return json.load(f)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[HTTP] {self.address_string()} {fmt % args}")

    def send_json(self, data: dict | list, status: int = 200):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type",  "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, path: Path, content_type: str):
        if not path.exists():
            self.send_response(404)
            self.end_headers()
            return
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type",  content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        def qp(key, default=""):
            return params.get(key, [default])[0]

        data = load_data()

        # ── API 라우팅 ──────────────────────────────────────────────────────
        if path == "/api/hot":
            limit = int(qp("limit", "10"))
            self.send_json(data.get("hot", [])[:limit])

        elif path == "/api/feed":
            sentiment = qp("sentiment", "all")
            limit     = int(qp("limit", "20"))
            feed      = data.get("feed", [])
            if sentiment != "all":
                feed = [i for i in feed if i.get("sentiment") == sentiment]
            self.send_json(feed[:limit])

        elif path == "/api/rising":
            limit = int(qp("limit", "10"))
            self.send_json(data.get("rising", [])[:limit])

        elif path == "/api/timeline":
            limit = int(qp("limit", "30"))
            self.send_json(data.get("timeline", [])[:limit])

        elif path == "/api/versus":
            self.send_json(data.get("versus", {}))

        elif path.startswith("/api/brand/"):
            brand = urllib.parse.unquote(path[len("/api/brand/"):])
            feed  = data.get("feed", [])
            result = [i for i in feed if brand in i.get("brand", "")]
            self.send_json(result[:20])

        elif path == "/api/search":
            q      = qp("q")
            feed   = data.get("feed", [])
            result = [
                i for i in feed
                if q in i.get("brand", "") or q in i.get("title", "")
            ]
            self.send_json(result[:20])

        elif path == "/api/status":
            self.send_json({
                "updated_at": data.get("updated_at", ""),
                "total":      len(data.get("feed", [])),
            })

        # ── 정적 파일 서빙 ─────────────────────────────────────────────────
        elif path == "/" or path == "/index.html":
            self.send_file(STATIC_ROOT / "index.html", "text/html; charset=utf-8")

        elif path == "/data/issues.json":
            self.send_file(DATA_FILE, "application/json; charset=utf-8")

        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    port = 8000
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"🌐 서버 실행 중 → http://localhost:{port}")
    print(f"   index.html: http://localhost:{port}/")
    print(f"   API 예시:   http://localhost:{port}/api/hot")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n서버 종료")
