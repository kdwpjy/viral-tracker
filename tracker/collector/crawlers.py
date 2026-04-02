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
    sort=1 → 최신순
    """
    url = f"https://search.naver.com/search.naver?where=news&query={quote(keyword)}&sort=1&ds=&de=&nso=so%3Add%2Cp%3Aall"
    posts = []

    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("div.news_wrap")

        for item in items[:15]:
            title_el = item.select_one("a.news_tit")
            desc_el  = item.select_one("div.dsc_wrap")
            press_el = item.select_one("a.press")
            date_el  = item.select_one("span.info")

            if not title_el:
                continue

            title = _clean(title_el.get_text())
            href  = title_el.get("href", "")
            body  = _clean(desc_el.get_text()) if desc_el else ""

            # post_id는 URL 해시로 대체
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
    """
    url = f"https://search.daum.net/search?w=news&q={quote(keyword)}&sort=recency"
    posts = []

    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("div.wrap_cont, li.item-list")

        for item in items[:15]:
            title_el = item.select_one("a.tit-doc, a[data-tiara-action-name='검색결과제목클릭']")
            desc_el  = item.select_one("p.desc-doc, p.desc")

            if not title_el:
                continue

            title   = _clean(title_el.get_text())
            href    = title_el.get("href", "")
            body    = _clean(desc_el.get_text()) if desc_el else ""
            post_id = re.sub(r"[^a-zA-Z0-9]", "", href)[-20:] or title[:20]

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
    공개 카페 게시글만 수집
    """
    url = f"https://search.daum.net/search?w=cafe&q={quote(keyword)}&sort=recency"
    posts = []

    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.select("li.item-board, div.c-item-search")

        for item in items[:15]:
            title_el = item.select_one("a.tit-doc, strong.tit-g")
            cafe_el  = item.select_one("span.info-cafe, a.cafe-name")
            desc_el  = item.select_one("p.desc-doc")

            if not title_el:
                continue

            title     = _clean(title_el.get_text())
            href      = title_el.get("href", "")
            cafe_name = _clean(cafe_el.get_text()) if cafe_el else ""
            body      = _clean(desc_el.get_text()) if desc_el else ""
            post_id   = re.sub(r"[^a-zA-Z0-9]", "", href)[-24:] or title[:20]

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
    url = f"https://bbs.ruliweb.com/search?searchkey={quote(keyword)}&searchtype=subject&page=1"
    posts = []

    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select("table.board_list_table tbody tr:not(.notice)")

        for row in rows[:15]:
            a = row.select_one("td.subject a.deco")
            if not a:
                continue

            title    = _clean(a.get_text())
            href     = a.get("href", "")
            hit_el   = row.select_one("td.hit")
            cmts_el  = row.select_one("td.reple_count")

            views    = _parse_int(hit_el.get_text() if hit_el else "")
            comments = _parse_int(cmts_el.get_text() if cmts_el else "")
            post_id  = (re.search(r"/(\d+)", href) or [None, title[:12]])[1]

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
