"""
API 키 없는 크롤러 모음
httpx + BeautifulSoup4 사용 (정적 HTML)
JS 렌더링이 필요한 경우 Playwright 사용
"""
import asyncio
import re
from datetime import datetime, timezone
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup

from .base import Channel, RawPost, detect_brands

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

CRAWL_DELAY = 2.0  # 초 (사이트 부하 방지)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _parse_int(text: str) -> int:
    try:
        return int(re.sub(r"[^\d]", "", text or "0") or "0")
    except ValueError:
        return 0


# ── 네이버 뉴스 (API 키 불필요 — 웹 검색 크롤링) ─────────────────────────────

async def crawl_naver_news(keyword: str) -> list[RawPost]:
    """
    https://search.naver.com/search.naver?where=news&query={keyword}&sort=1
    네이버 뉴스 검색 결과 페이지 크롤링 (sds-comps-base-layout 컨테이너 기반)
    """
    url = f"https://search.naver.com/search.naver?where=news&query={quote(keyword)}&sort=1"
    posts = []

    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        seen_hrefs: set[str] = set()

        for container in soup.select("div.sds-comps-base-layout"):
            links = container.find_all("a", href=True)
            news_links = [
                a for a in links
                if a.get("href", "").startswith("http")
                and len(_clean(a.get_text())) > 15
                and "naver.com/search" not in a.get("href", "")
                and "naver.com/main" not in a.get("href", "")
            ]
            if not news_links:
                continue

            # 첫 번째 긴 텍스트 링크 = 제목
            title_a = news_links[0]
            href    = title_a.get("href", "")
            title   = _clean(title_a.get_text())

            if not title or href in seen_hrefs:
                continue
            seen_hrefs.add(href)

            # 이후 링크 중 가장 긴 텍스트 = 요약
            desc_candidates = [_clean(a.get_text()) for a in news_links[1:] if len(_clean(a.get_text())) > 20]
            body = desc_candidates[0] if desc_candidates else title

            post_id = re.sub(r"[^a-zA-Z0-9]", "", href)[-20:] or title[:20]
            posts.append(RawPost(
                channel      = Channel.NAVER_NEWS,
                post_id      = post_id,
                url          = href,
                title        = title,
                body         = body,
                published_at = datetime.now(timezone.utc),
            ))

        await asyncio.sleep(CRAWL_DELAY)
    except Exception as e:
        print(f"[NaverNews] '{keyword}' 수집 실패: {e}")

    return posts


# ── 다음 뉴스 ─────────────────────────────────────────────────────────────────

async def crawl_daum_news(keyword: str) -> list[RawPost]:
    """
    https://search.daum.net/search?w=news&q={keyword}&sort=recency
    div.item-bundle-mid 컨테이너 기반 파싱
    """
    url = f"https://search.daum.net/search?w=news&q={quote(keyword)}&sort=recency"
    posts = []

    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("div.item-bundle-mid")

        for item in items[:15]:
            title_el = item.select_one("div.item-title a")
            desc_el  = item.select_one("p.conts-desc")

            if not title_el:
                continue

            title   = _clean(title_el.get_text())
            href    = title_el.get("href", "")
            body    = _clean(desc_el.get_text()) if desc_el else title
            post_id = re.sub(r"[^a-zA-Z0-9]", "", href)[-20:] or title[:20]

            if not title:
                continue

            posts.append(RawPost(
                channel      = Channel.DAUM_NEWS,
                post_id      = post_id,
                url          = href,
                title        = title,
                body         = body,
                published_at = datetime.now(timezone.utc),
            ))

        await asyncio.sleep(CRAWL_DELAY)
    except Exception as e:
        print(f"[DaumNews] '{keyword}' 수집 실패: {e}")

    return posts


# ── 다음 카페 (공개 게시글 검색) ──────────────────────────────────────────────

async def crawl_daum_cafe(keyword: str) -> list[RawPost]:
    """
    https://search.daum.net/search?w=cafe&q={keyword}&sort=recency
    다음 뉴스와 동일한 item-bundle-mid 구조 사용
    """
    # w=cafe 는 JS 리다이렉트로 변경됨 → w=tot(통합검색)으로 카페 결과 포함 수집
    url = f"https://search.daum.net/search?w=tot&q={quote(keyword)}&sort=recency"
    posts = []

    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("div.item-bundle-mid")

        for item in items[:15]:
            title_el = item.select_one("div.item-title a")
            desc_el  = item.select_one("p.conts-desc")
            cafe_el  = item.select_one("a.cafe-name, span.cafe-name, div.item-etc a")

            if not title_el:
                continue

            title     = _clean(title_el.get_text())
            href      = title_el.get("href", "")
            cafe_name = _clean(cafe_el.get_text()) if cafe_el else ""
            body      = _clean(desc_el.get_text()) if desc_el else title
            post_id   = re.sub(r"[^a-zA-Z0-9]", "", href)[-24:] or title[:20]

            if not title:
                continue

            posts.append(RawPost(
                channel      = Channel.DAUM_CAFE,
                post_id      = post_id,
                url          = href,
                title        = f"[{cafe_name}] {title}" if cafe_name else title,
                body         = body,
                published_at = datetime.now(timezone.utc),
            ))

        await asyncio.sleep(CRAWL_DELAY)
    except Exception as e:
        print(f"[DaumCafe] '{keyword}' 수집 실패: {e}")

    return posts


# ── 루리웹 (서버사이드 렌더링 — requests 가능) ───────────────────────────────

async def crawl_ruliweb(keyword: str) -> list[RawPost]:
    """
    루리웹 /search 는 JS렌더링 → Playwright 사용
    Playwright 미설치 시 건너뜀
    """
    posts = []
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("[Ruliweb] playwright 미설치 — 건너뜀")
        return posts

    url = f"https://bbs.ruliweb.com/search?searchkey={quote(keyword)}&searchtype=subject&page=1"

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page    = await browser.new_page(user_agent=HEADERS["User-Agent"])
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            rows = await page.query_selector_all("tr.table_body")
            for row in rows[:15]:
                a = await row.query_selector("td.subject a.deco")
                if not a:
                    continue
                title    = _clean(await a.inner_text())
                href     = await a.get_attribute("href") or ""
                if not href.startswith("http"):
                    href = "https://bbs.ruliweb.com" + href

                hit_el  = await row.query_selector("td.hit")
                cmts_el = await row.query_selector("td.reple_count")
                views    = _parse_int(await hit_el.inner_text() if hit_el else "")
                comments = _parse_int(await cmts_el.inner_text() if cmts_el else "")
                post_id  = (re.search(r"/(\d+)", href) or [None, title[:12]])[1]

                if not title:
                    continue

                posts.append(RawPost(
                    channel      = Channel.RULIWEB,
                    post_id      = str(post_id),
                    url          = href,
                    title        = title,
                    body         = title,
                    published_at = datetime.now(timezone.utc),
                    views        = views,
                    comments     = comments,
                ))

            await browser.close()
            await asyncio.sleep(CRAWL_DELAY)
    except Exception as e:
        print(f"[Ruliweb] '{keyword}' 수집 실패: {e}")

    return posts


# ── 에펨코리아 (JS 렌더링 필요 → Playwright) ────────────────────────────────

async def crawl_fmkorea(keyword: str) -> list[RawPost]:
    """
    에펨코리아는 XE 기반이나 검색 결과가 JS로 로드됨.
    Playwright가 설치되지 않은 경우 빈 리스트 반환.
    """
    posts = []
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("[FMKorea] playwright 미설치 — 건너뜀 (pip install playwright && playwright install chromium)")
        return posts

    url = f"https://www.fmkorea.com/search?searchTarget=title&keyword={quote(keyword)}"

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page    = await browser.new_page(user_agent=HEADERS["User-Agent"])
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            rows = await page.query_selector_all("ul.searchResultList > li")
            for row in rows[:15]:
                a = await row.query_selector("h3.title a")
                if not a:
                    continue

                title    = _clean(await a.inner_text())
                href     = await a.get_attribute("href") or ""
                if href.startswith("/"):
                    href = "https://www.fmkorea.com" + href

                view_el = await row.query_selector(".count_view")
                cmts_el = await row.query_selector(".count_comment")
                views    = _parse_int(await view_el.inner_text() if view_el else "")
                comments = _parse_int(await cmts_el.inner_text() if cmts_el else "")
                post_id  = (re.search(r"/(\d+)", href) or [None, title[:12]])[1]

                posts.append(RawPost(
                    channel      = Channel.FMKOREA,
                    post_id      = str(post_id),
                    url          = href,
                    title        = title,
                    body         = title,
                    published_at = datetime.now(timezone.utc),
                    views        = views,
                    comments     = comments,
                ))

            await browser.close()
            await asyncio.sleep(CRAWL_DELAY)
    except Exception as e:
        print(f"[FMKorea] '{keyword}' 수집 실패: {e}")

    return posts


# ── 클리앙 ────────────────────────────────────────────────────────────────────

async def crawl_clien(keyword: str) -> list[RawPost]:
    """
    클리앙 소비자 게시판 검색 (networkidle 필요)
    Playwright 없으면 건너뜀
    """
    posts = []
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return posts

    url = f"https://www.clien.net/service/search?q={quote(keyword)}&sort=recency&boardName=cm_consumer"

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            page    = await browser.new_page(user_agent=HEADERS["User-Agent"])
            await page.goto(url, wait_until="networkidle", timeout=25000)
            await asyncio.sleep(1.5)

            items = await page.query_selector_all("div.list_item")
            for item in items[:15]:
                a = await item.query_selector("span.subject_fixed a")
                if not a:
                    continue

                title    = _clean(await a.inner_text())
                href     = "https://www.clien.net" + (await a.get_attribute("href") or "")
                hit_el   = await item.query_selector("span.hit")
                cmts_el  = await item.query_selector("span.rSymbol")
                views    = _parse_int(await hit_el.inner_text() if hit_el else "")
                comments = _parse_int(await cmts_el.inner_text() if cmts_el else "")
                post_id  = (re.search(r"(\d+)$", href) or [None, title[:12]])[1]

                posts.append(RawPost(
                    channel      = Channel.CLIEN,
                    post_id      = str(post_id),
                    url          = href,
                    title        = title,
                    body         = title,
                    published_at = datetime.now(timezone.utc),
                    views        = views,
                    comments     = comments,
                ))

            await browser.close()
            await asyncio.sleep(CRAWL_DELAY)
    except Exception as e:
        print(f"[Clien] '{keyword}' 수집 실패: {e}")

    return posts


# ── 통합 수집 함수 ────────────────────────────────────────────────────────────

CRAWLERS = [
    crawl_naver_news,
    crawl_daum_news,
    crawl_daum_cafe,
    crawl_ruliweb,
    crawl_fmkorea,
    crawl_clien,
]

async def collect_keyword(keyword: str) -> list[RawPost]:
    """하나의 키워드를 모든 채널에서 수집 (순차 실행 — 사이트 부하 방지)"""
    all_posts = []
    for crawl_fn in CRAWLERS:
        try:
            posts = await crawl_fn(keyword)
            all_posts.extend(posts)
        except Exception as e:
            print(f"[{crawl_fn.__name__}] 오류: {e}")
    return all_posts
