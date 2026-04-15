"""
SQLite 저장 + JSON 내보내기
GitHub Pages용: data/issues.json 을 프로젝트 루트에 생성
"""
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path

from tracker.processor.analyzer import ProcessedIssue

# 프로젝트 루트 기준으로 경로 설정 (GitHub Actions 환경 대응)
ROOT     = Path(__file__).parent.parent.parent
DB_PATH  = ROOT / "tracker" / "data" / "tracker.db"
JSON_PATH = ROOT / "data" / "issues.json"


def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_conn() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS issues (
            uid          TEXT PRIMARY KEY,
            brand        TEXT,
            channel      TEXT,
            url          TEXT,
            title        TEXT,
            summary      TEXT,
            sentiment    TEXT,
            tags         TEXT,
            viral_score  REAL,
            status       TEXT,
            stakeholders TEXT,
            published_at TEXT,
            processed_at TEXT,
            views        INTEGER DEFAULT 0,
            comments     INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_brand     ON issues(brand);
        CREATE INDEX IF NOT EXISTS idx_sentiment ON issues(sentiment);
        CREATE INDEX IF NOT EXISTS idx_score     ON issues(viral_score DESC);
        """)


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def save_issue(issue: ProcessedIssue):
    d = asdict(issue)
    d["tags"]         = json.dumps(d["tags"],         ensure_ascii=False)
    d["stakeholders"] = json.dumps(d["stakeholders"], ensure_ascii=False)
    with get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO issues
            (uid, brand, channel, url, title, summary, sentiment,
             tags, viral_score, status, stakeholders, published_at, processed_at, views, comments)
            VALUES (:uid,:brand,:channel,:url,:title,:summary,:sentiment,
                    :tags,:viral_score,:status,:stakeholders,:published_at,:processed_at,:views,:comments)
        """, d)


def _to_dicts(rows) -> list[dict]:
    result = []
    for row in rows:
        d = dict(row)
        for key in ("tags", "stakeholders"):
            try:
                d[key] = json.loads(d[key])
            except Exception:
                d[key] = []
        result.append(d)
    return result


def get_hot(limit=10) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM issues ORDER BY viral_score DESC LIMIT ?", (limit,)
        ).fetchall()
    return _to_dicts(rows)


def get_by_sentiment(sentiment: str, limit=20) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM issues WHERE sentiment=? ORDER BY viral_score DESC LIMIT ?",
            (sentiment, limit)
        ).fetchall()
    return _to_dicts(rows)


def get_rising(limit=10) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM issues WHERE status IN ('Hot','Rising') ORDER BY viral_score DESC LIMIT ?",
            (limit,)
        ).fetchall()
    return _to_dicts(rows)


def get_timeline(limit=30) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM issues ORDER BY published_at DESC LIMIT ?", (limit,)
        ).fetchall()
    return _to_dicts(rows)


def get_versus() -> dict:
    def by_brand(name, limit=20):
        with get_conn() as conn:
            rows = conn.execute(
                "SELECT * FROM issues WHERE brand LIKE ? ORDER BY published_at DESC LIMIT ?",
                (f"%{name}%", limit)
            ).fetchall()
        return _to_dicts(rows)

    baemin  = by_brand("배달의민족") + by_brand("배민")
    coupang = by_brand("쿠팡이츠")

    def avg_score(items):
        return round(sum(i["viral_score"] for i in items) / len(items), 1) if items else 0

    def ratio(items):
        total = len(items) or 1
        return {
            "negative": round(sum(1 for i in items if i["sentiment"] == "negative") / total * 100),
            "positive": round(sum(1 for i in items if i["sentiment"] == "positive") / total * 100),
            "meme":     round(sum(1 for i in items if i["sentiment"] == "meme")     / total * 100),
        }

    return {
        "baemin":  {"issues": baemin[:5],  "total": len(baemin),  "avg_score": avg_score(baemin),  "ratio": ratio(baemin)},
        "coupang": {"issues": coupang[:5], "total": len(coupang), "avg_score": avg_score(coupang), "ratio": ratio(coupang)},
    }


def export_json():
    """GitHub Pages가 읽을 data/issues.json 생성"""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    data = {
        "updated_at": now.isoformat(),
        "hot":        get_hot(10),
        "feed":       get_timeline(60),
        "rising":     get_rising(10),
        "timeline":   get_timeline(30),
        "versus":     get_versus(),
    }
    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[DB] data/issues.json 저장 완료 ({len(data['feed'])}건)")

    # 시계열 스냅샷 저장 (data/history/YYYY-MM-DDTHH.json)
    snapshot_dir = JSON_PATH.parent / "history"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = snapshot_dir / f"{now.strftime('%Y-%m-%dT%H')}.json"
    with open(snapshot_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[DB] 스냅샷 저장 완료 → {snapshot_path.name}")
