"""
수집 파이프라인
  - --once 플래그: GitHub Actions에서 1회 실행 후 종료
  - 플래그 없음:   로컬에서 1시간 간격 반복 실행
"""
import argparse
import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가 (GitHub Actions 환경 대응)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from datetime import datetime, timedelta, timezone

from tracker.collector.base import SEARCH_KEYWORDS, PRIORITY_KEYWORDS, detect_brands
from tracker.collector.crawlers import collect_keyword, fetch_actual_dates
from tracker.processor.analyzer import analyze_posts
from tracker.storage.db import init_db, save_issue, export_json

FEED_CUTOFF_DAYS = 7   # 피드: 7일 이내
HOT_CUTOFF_HOURS = 24  # hot/versus: 24시간 이내

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("pipeline")


async def run():
    log.info(f"🚀 수집 시작 [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")

    all_posts = []
    seen_uids = set()

    # 1) 일반 키워드 수집
    for kw in SEARCH_KEYWORDS:
        log.info(f"  📥 '{kw}' 수집 중...")
        posts = await collect_keyword(kw)
        for p in posts:
            if p.uid not in seen_uids:
                seen_uids.add(p.uid)
                all_posts.append(p)

    # 2) 배민·쿠팡이츠 우선 수집
    log.info("  🍔 배민·쿠팡이츠 전용 키워드 수집 중...")
    for kw in PRIORITY_KEYWORDS:
        posts = await collect_keyword(kw)
        for p in posts:
            if p.uid not in seen_uids:
                seen_uids.add(p.uid)
                all_posts.append(p)

    log.info(f"📥 총 {len(all_posts)}건 수집 완료")

    # 3) 실제 발행일 보정 (뉴스·커뮤니티 기사 URL fetch) — 필터 전에 실행
    log.info(f"📅 발행일 보정 중 (전체 {len(all_posts)}건)...")
    await fetch_actual_dates(all_posts, concurrency=5)

    # 4) 7일 초과 게시글 필터 (발행일 보정 후 적용)
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=FEED_CUTOFF_DAYS)
    all_posts = [
        p for p in all_posts
        if p.published_at.replace(tzinfo=timezone.utc) >= cutoff
    ]
    log.info(f"📥 7일 이내 {len(all_posts)}건 필터링 완료")

    # 5) 분석
    issues = analyze_posts(all_posts)
    log.info(f"🔍 {len(issues)}건 분석 완료")

    # 6) 저장 + JSON 내보내기
    for issue in issues:
        save_issue(issue)

    export_json()
    log.info("✅ 완료")


async def run_scheduled(interval_hours: int = 1):
    while True:
        try:
            await run()
        except Exception as e:
            log.error(f"파이프라인 오류: {e}")
        log.info(f"⏰ {interval_hours}시간 후 재실행...")
        await asyncio.sleep(interval_hours * 3600)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="1회 실행 후 종료 (GitHub Actions용)")
    args = parser.parse_args()

    init_db()

    if args.once:
        asyncio.run(run())
    else:
        asyncio.run(run_scheduled(interval_hours=1))
