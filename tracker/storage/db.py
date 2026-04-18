"""
SQLite 저장 + JSON 내보내기
GitHub Pages용: data/issues.json 을 프로젝트 루트에 생성
모든 datetime 은 KST aware (Asia/Seoul, UTC+9) 기준.
"""
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

from tracker.collector.base import KST, now_kst
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


_INSERT_SQL = """
    INSERT OR REPLACE INTO issues
    (uid, brand, channel, url, title, summary, sentiment,
     tags, viral_score, status, stakeholders, published_at, processed_at, views, comments)
    VALUES (:uid,:brand,:channel,:url,:title,:summary,:sentiment,
            :tags,:viral_score,:status,:stakeholders,:published_at,:processed_at,:views,:comments)
"""


def _issue_to_row(issue: ProcessedIssue) -> dict:
    d = asdict(issue)
    d["tags"]         = json.dumps(d["tags"],         ensure_ascii=False)
    d["stakeholders"] = json.dumps(d["stakeholders"], ensure_ascii=False)
    return d


def save_issue(issue: ProcessedIssue):
    with get_conn() as conn:
        conn.execute(_INSERT_SQL, _issue_to_row(issue))


def save_issues_bulk(issues: list[ProcessedIssue]) -> int:
    """단일 트랜잭션 + executemany로 N건 일괄 저장 (M5 성능 최적화)"""
    if not issues:
        return 0
    rows = [_issue_to_row(i) for i in issues]
    with get_conn() as conn:
        conn.executemany(_INSERT_SQL, rows)
    return len(rows)


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
    cutoff = (now_kst() - timedelta(hours=24)).isoformat()
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM issues WHERE published_at >= ? ORDER BY viral_score DESC LIMIT ?",
            (cutoff, limit)
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
    cutoff = (now_kst() - timedelta(hours=24)).isoformat()

    def by_brand_any(names: list[str], limit=20):
        like_clauses = " OR ".join("brand LIKE ?" for _ in names)
        params = [f"%{n}%" for n in names] + [cutoff, limit]
        with get_conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM issues WHERE ({like_clauses}) AND published_at >= ? "
                f"ORDER BY published_at DESC LIMIT ?",
                params,
            ).fetchall()
        # uid 기준 dedup (같은 게시글이 여러 LIKE에 걸리는 경우 방지)
        seen, deduped = set(), []
        for d in _to_dicts(rows):
            if d["uid"] in seen:
                continue
            seen.add(d["uid"])
            deduped.append(d)
        return deduped

    baemin  = by_brand_any(["배달의민족", "배민"])
    coupang = by_brand_any(["쿠팡이츠"])

    def avg_score(items):
        return round(sum(i["viral_score"] for i in items) / len(items), 1) if items else 0

    def ratio(items):
        total = len(items) or 1
        return {
            "negative": round(sum(1 for i in items if i["sentiment"] == "negative") / total * 100),
            "positive": round(sum(1 for i in items if i["sentiment"] == "positive") / total * 100),
            "meme":     round(sum(1 for i in items if i["sentiment"] == "meme")     / total * 100),
        }

    # 전체 수집 기사에서 정규식 기반 언급 횟수 집계
    from tracker.collector.base import BRAND_MENTION_PATTERNS
    with get_conn() as conn:
        all_rows = conn.execute("SELECT title, summary FROM issues").fetchall()
    mention_counts = {}
    for brand, pattern in BRAND_MENTION_PATTERNS.items():
        mention_counts[brand] = sum(
            1 for row in all_rows
            if pattern.search((row["title"] or "") + " " + (row["summary"] or ""))
        )

    return {
        "baemin":  {"issues": baemin[:5],  "total": len(baemin),  "avg_score": avg_score(baemin),  "ratio": ratio(baemin),  "mentions": mention_counts.get("배달의민족", 0)},
        "coupang": {"issues": coupang[:5], "total": len(coupang), "avg_score": avg_score(coupang), "ratio": ratio(coupang), "mentions": mention_counts.get("쿠팡이츠",   0)},
    }


def _parse_iso(s: str) -> datetime:
    """ISO 문자열(KST 또는 UTC) → KST aware datetime. 실패 시 epoch 반환 (정렬에서 가장 뒤로)."""
    try:
        dt = datetime.fromisoformat((s or "").replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except (ValueError, TypeError):
        return datetime.min.replace(tzinfo=KST)


def _build_weekly_feed(now: datetime) -> list[dict]:
    """현재 DB + 최근 7일 히스토리 스냅샷을 uid 기준 중복 제거 후 합산.
    혼합 tz(과거 UTC + 신규 KST) 방어: 정렬·cutoff 모두 datetime 비교 사용."""
    cutoff = now - timedelta(days=7)

    # 현재 실행 데이터
    with get_conn() as conn:
        rows = conn.execute("SELECT * FROM issues ORDER BY published_at DESC").fetchall()
    seen: dict[str, dict] = {item["uid"]: item for item in _to_dicts(rows)}

    # 히스토리 스냅샷 병합 (최신 → 오래된 순)
    history_dir = JSON_PATH.parent / "history"
    if history_dir.exists():
        # 파일명 stem은 KST 또는 (과거) UTC hour. 먼저 datetime으로 파싱 후 정렬.
        snap_with_ts = []
        for snap_file in history_dir.glob("*.json"):
            try:
                ts = datetime.fromisoformat(snap_file.stem)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=KST)  # 신규 KST 가정 (과거 파일도 9시간 차이는 sort에서만 영향)
            except Exception:
                continue
            snap_with_ts.append((ts, snap_file))
        snap_with_ts.sort(key=lambda x: x[0], reverse=True)

        for ts, snap_file in snap_with_ts:
            if ts < cutoff:
                break
            with open(snap_file, encoding="utf-8") as fp:
                snap = json.load(fp)
            for item in snap.get("feed", []):
                uid = item.get("uid")
                if uid and uid not in seen:
                    seen[uid] = item

    result = sorted(seen.values(), key=lambda x: _parse_iso(x.get("published_at", "")), reverse=True)
    return result[:300]


def export_json():
    """GitHub Pages가 읽을 data/issues.json 생성 (KST 기준)"""
    now = now_kst()
    data = {
        "updated_at": now.isoformat(),
        "hot":        get_hot(10),        # 당일 수집 기준
        "feed":       _build_weekly_feed(now),  # 최근 7일 누적
        "versus":     get_versus(),       # 당일 수집 기준
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

    _prune_history(snapshot_dir, now)


# 7일 누적 피드 + 약간의 여유 → 10일 이상 된 스냅샷 삭제
HISTORY_RETENTION_DAYS = 10


def _prune_history(snapshot_dir: Path, now: datetime) -> int:
    """오래된 history 스냅샷 삭제 (M2: 리포 비대화 방지)"""
    cutoff = now - timedelta(days=HISTORY_RETENTION_DAYS)
    removed = 0
    for f in snapshot_dir.glob("*.json"):
        try:
            ts = datetime.fromisoformat(f.stem)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=KST)
        except Exception:
            continue   # 비정상 파일명은 보존 (수동 분석용일 수 있음)
        if ts < cutoff:
            try:
                f.unlink()
                removed += 1
            except OSError:
                pass
    if removed:
        print(f"[DB] 오래된 스냅샷 {removed}개 삭제 (>{HISTORY_RETENTION_DAYS}일)")
    return removed
