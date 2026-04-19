"""
수집 파이프라인
  - --once 플래그: GitHub Actions에서 1회 실행 후 종료
  - 플래그 없음:   로컬에서 1시간 간격 반복 실행
"""
import argparse
import asyncio
import logging
import sys
from datetime import timedelta
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가 (GitHub Actions 환경 대응)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from tracker.collector.base import KST, SEARCH_KEYWORDS, PRIORITY_KEYWORDS, detect_brands, now_kst
from tracker.collector.crawlers import collect_keyword, fetch_actual_dates
from tracker.processor.analyzer import analyze_posts
from tracker.storage.db import init_db, save_issues_bulk, export_json

FEED_CUTOFF_DAYS = 7   # 피드: 7일 이내
HOT_CUTOFF_HOURS = 24  # hot/versus: 24시간 이내

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("pipeline")


async def run():
    log.info(f"🚀 수집 시작 [{now_kst().strftime('%Y-%m-%d %H:%M')} KST]")

    # uid 기준 중복 제거하되, 같은 글이 여러 키워드에 걸리면 matched_keywords 누적
    by_uid: dict[str, object] = {}

    def _merge(posts):
        for p in posts:
            if p.uid in by_uid:
                exist = by_uid[p.uid]
                for kw in p.matched_keywords:
                    if kw not in exist.matched_keywords:
                        exist.matched_keywords.append(kw)
            else:
                by_uid[p.uid] = p

    # 1) 일반 키워드 수집
    for kw in SEARCH_KEYWORDS:
        log.info(f"  📥 '{kw}' 수집 중...")
        _merge(await collect_keyword(kw))

    # 2) 배민·쿠팡이츠 우선 수집
    log.info("  🍔 배민·쿠팡이츠 전용 키워드 수집 중...")
    for kw in PRIORITY_KEYWORDS:
        _merge(await collect_keyword(kw))

    all_posts = list(by_uid.values())
    log.info(f"📥 총 {len(all_posts)}건 수집 완료")

    # 3) 실제 발행일 보정 (뉴스·커뮤니티 기사 URL fetch) — 필터 전에 실행
    log.info(f"📅 발행일 보정 중 (전체 {len(all_posts)}건)...")
    await fetch_actual_dates(all_posts, concurrency=5)

    # 4) 7일 초과 게시글 필터 (발행일 보정 후 적용, KST 기준)
    cutoff = now_kst() - timedelta(days=FEED_CUTOFF_DAYS)

    def _aware_kst(dt):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=KST)
        return dt.astimezone(KST)

    all_posts = [p for p in all_posts if _aware_kst(p.published_at) >= cutoff]
    log.info(f"📥 7일 이내 {len(all_posts)}건 필터링 완료")

    # 5) 분석
    issues = analyze_posts(all_posts)
    log.info(f"🔍 {len(issues)}건 분석 완료")

    # 6) 저장 + JSON 내보내기 (M5: 단일 트랜잭션 일괄 저장)
    saved = save_issues_bulk(issues)
    log.info(f"💾 {saved}건 저장 완료")

    export_json()
    log.info("✅ 완료")

    # L3: 모든 채널 차단/장애 감지 — 한 건도 못 받으면 Actions 실패로 알림
    if saved == 0:
        log.error("⚠️  수집된 이슈가 0건입니다. 모든 채널이 차단됐거나 셀렉터가 깨졌을 수 있습니다.")
        sys.exit(1)


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
