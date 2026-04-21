"""
SQLite 저장 + JSON 내보내기
GitHub Pages용: data/issues.json 을 프로젝트 루트에 생성
모든 datetime 은 KST aware (Asia/Seoul, UTC+9) 기준.
"""
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

from tracker.collector.base import KST, now_kst, BRAND_MENTION_PATTERNS

# 브랜드 패턴 외 제목 관련성 판단에 쓸 비브랜드 키워드
_NON_BRAND_KWS = ["배달비", "무료배달", "단건배달", "배달앱"]

def _title_relevant(item: dict) -> bool:
    title = item.get("title") or ""
    if any(p.search(title) for p in BRAND_MENTION_PATTERNS.values()):
        return True
    tl = title.lower()
    return any(kw in tl for kw in _NON_BRAND_KWS)
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
            uid               TEXT PRIMARY KEY,
            brand             TEXT,
            channel           TEXT,
            url               TEXT,
            title             TEXT,
            summary           TEXT,
            sentiment         TEXT,
            tags              TEXT,
            viral_score       REAL,
            status            TEXT,
            stakeholders      TEXT,
            published_at      TEXT,
            processed_at      TEXT,
            views             INTEGER DEFAULT 0,
            comments          INTEGER DEFAULT 0,
            matched_keywords  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_brand     ON issues(brand);
        CREATE INDEX IF NOT EXISTS idx_sentiment ON issues(sentiment);
        CREATE INDEX IF NOT EXISTS idx_score     ON issues(viral_score DESC);
        """)
        # 구버전 DB 마이그레이션 (컬럼 없으면 추가)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(issues)").fetchall()}
        if "matched_keywords" not in cols:
            conn.execute("ALTER TABLE issues ADD COLUMN matched_keywords TEXT")


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
     tags, viral_score, status, stakeholders, published_at, processed_at,
     views, comments, matched_keywords)
    VALUES (:uid,:brand,:channel,:url,:title,:summary,:sentiment,
            :tags,:viral_score,:status,:stakeholders,:published_at,:processed_at,
            :views,:comments,:matched_keywords)
"""


def _issue_to_row(issue: ProcessedIssue) -> dict:
    d = asdict(issue)
    d["tags"]             = json.dumps(d["tags"],             ensure_ascii=False)
    d["stakeholders"]     = json.dumps(d["stakeholders"],     ensure_ascii=False)
    d["matched_keywords"] = json.dumps(d.get("matched_keywords", []), ensure_ascii=False)
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
        for key in ("tags", "stakeholders", "matched_keywords"):
            try:
                d[key] = json.loads(d[key])
            except Exception:
                d[key] = []
        result.append(d)
    return result


# ── 유사 기사 병합 ────────────────────────────────────────────────────────────
# 같은 사건을 여러 매체가 재게재하는 경우 (예: 연합뉴스 wire + 각 언론사) 대표
# 하나만 남기고 duplicate_count/_urls 를 기록. 문자 n-gram Jaccard 유사도 사용.

DEDUP_THRESHOLD = float(os.environ.get("DEDUP_THRESHOLD", "0.2"))
_NGRAM_N = 2   # 한국어 짧은 제목엔 2-gram 이 조사/어미 변화에 더 관대


def _char_ngrams(text: str, n: int = _NGRAM_N) -> set:
    """한국어 친화적 n-gram: 기호/공백 제거 후 문자 단위로 n-gram 생성."""
    clean = re.sub(r"[^\w가-힣]", "", (text or ""))
    if len(clean) < n:
        return {clean} if clean else set()
    return {clean[i:i + n] for i in range(len(clean) - n + 1)}


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _min_containment(a: set, b: set) -> float:
    """두 집합 중 작은 쪽 기준 공통 비율. 짧은 제목 간 포함 관계 감지에 유리."""
    if not a or not b:
        return 0.0
    return len(a & b) / min(len(a), len(b))


# 병합 조건: Jaccard >= THRESHOLD 이거나 min-containment >= CONTAIN_THRESHOLD
CONTAIN_THRESHOLD = float(os.environ.get("DEDUP_CONTAIN", "0.6"))


def _title_similar(a_ng: set, b_ng: set) -> bool:
    if _jaccard(a_ng, b_ng) >= DEDUP_THRESHOLD:
        return True
    if _min_containment(a_ng, b_ng) >= CONTAIN_THRESHOLD:
        return True
    return False


def _dedup_by_title(items: list[dict], threshold: float = DEDUP_THRESHOLD) -> list[dict]:
    """
    제목 유사도(char n-gram Jaccard) 기반 중복 병합.

    - 대표(rep)는 각 group에서 viral_score 최고인 항목.
    - 반환 순서는 **호출자가 전달한 원본 순서를 유지** (published_at 정렬 등 보존).
    - 각 대표에 duplicate_count / duplicate_urls 필드 추가.
    """
    # Pass 1: viral_score 내림차순으로 group 구성 → rep 선정
    by_score = sorted(
        enumerate(items),
        key=lambda t: t[1].get("viral_score", 0) or 0,
        reverse=True,
    )
    group_rep_idx: list[int] = []       # 각 group 의 대표 원본 인덱스
    group_ngrams: list[set] = []
    group_dup_idx: list[list[int]] = [] # group 별 중복 원본 인덱스들
    item_to_group: dict[int, int] = {}

    for orig_idx, it in by_score:
        ng = _char_ngrams(it.get("title", ""))
        matched_gi = None
        for gi, rep_ng in enumerate(group_ngrams):
            if _title_similar(ng, rep_ng):
                matched_gi = gi
                break
        if matched_gi is None:
            matched_gi = len(group_ngrams)
            group_ngrams.append(ng)
            group_rep_idx.append(orig_idx)
            group_dup_idx.append([])
        else:
            group_dup_idx[matched_gi].append(orig_idx)
        item_to_group[orig_idx] = matched_gi

    # Pass 2: 원본 순서대로 순회하되, group 은 처음 등장 시만 emit
    seen_groups: set[int] = set()
    result = []
    for orig_idx in range(len(items)):
        gi = item_to_group[orig_idx]
        if gi in seen_groups:
            continue
        seen_groups.add(gi)
        rep = dict(items[group_rep_idx[gi]])
        dup_idx = group_dup_idx[gi]
        rep["duplicate_count"] = len(dup_idx)
        # 유사 기사 상세 (프론트에서 +N 유사 뱃지 클릭 시 expand 표시)
        rep["duplicates"] = [
            {
                "title":   items[di].get("title", ""),
                "url":     items[di].get("url", ""),
                "channel": items[di].get("channel", ""),
            }
            for di in dup_idx
        ]
        result.append(rep)
    return result


def get_hot(limit=10) -> list[dict]:
    cutoff = (now_kst() - timedelta(hours=24)).isoformat()
    with get_conn() as conn:
        # dedup 후 limit 맞추기 위해 여유있게 3배수 조회
        rows = conn.execute(
            "SELECT * FROM issues WHERE published_at >= ? ORDER BY viral_score DESC LIMIT ?",
            (cutoff, max(limit * 3, 30))
        ).fetchall()
    items = [i for i in _to_dicts(rows) if _title_relevant(i)]
    return _dedup_by_title(items)[:limit]


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

    # versus 섹션은 각 브랜드 '단독' 이슈만 표시 — 두 브랜드 동시 언급된 공통
    # 이슈(brand='배달의민족, 쿠팡이츠')는 양쪽 모두에서 제외.
    def _only(items, exclude_keys):
        return [i for i in items
                if not any(k in (i.get("brand") or "") for k in exclude_keys)]

    baemin_rows  = _only(by_brand_any(["배달의민족", "배민"]), exclude_keys=["쿠팡이츠"])
    coupang_rows = _only(by_brand_any(["쿠팡이츠"]),            exclude_keys=["배달의민족", "배민"])
    baemin  = _dedup_by_title(baemin_rows)
    coupang = _dedup_by_title(coupang_rows)

    def avg_score(items):
        return round(sum(i["viral_score"] for i in items) / len(items), 1) if items else 0

    def ratio(items):
        total = len(items) or 1
        return {
            "negative": round(sum(1 for i in items if i["sentiment"] == "negative") / total * 100),
            "positive": round(sum(1 for i in items if i["sentiment"] == "positive") / total * 100),
            "neutral":  round(sum(1 for i in items if i["sentiment"] == "neutral")  / total * 100),
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
                    item.setdefault("matched_keywords", [])
                    seen[uid] = item

    # 1) 제목 관련성 필터 (파이프라인과 동일 규칙)
    relevant = {uid: item for uid, item in seen.items() if _title_relevant(item)}

    # 2) 최신순 정렬 → 3) 제목 유사도 중복 병합 → 4) 300건 cap
    by_time = sorted(relevant.values(), key=lambda x: _parse_iso(x.get("published_at", "")), reverse=True)
    deduped = _dedup_by_title(by_time)
    return deduped[:300]


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
