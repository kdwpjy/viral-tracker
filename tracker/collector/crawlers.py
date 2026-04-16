"""
API 키 없는 크롤러 모음
httpx + BeautifulSoup4 사용 (정적 HTML)
JS 렌더링이 필요한 경우 Playwright 사용
"""
import asyncio
import os
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import httpx
from bs4 import BeautifulSoup

from .base import Channel, RawPost, detect_brands

# 로컬: channel="chrome" (시스템 Chrome), CI: channel="chromium" (설치된 Chromium)
_PW_CHANNEL = os.environ.get("PLAYWRIGHT_CHANNEL", "chrome")

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


def _parse_date(text: str) -> datetime:
    """
    사이트별 날짜 텍스트를 datetime(UTC)으로 변환.
    파싱 실패 시 현재 시각 반환.
    지원 형식:
      "16:30"          → 오늘
      "04.16"          → 올해 4월 16일
      "26.04.16"       → 2026-04-16
      "2026.04.16"     → 2026-04-16
      "2026-04-16"     → 2026-04-16
      "2026/04/16"     → 2026-04-16
      "N분/시간/일 전"  → 상대 시각
    """
    now = datetime.now(timezone.utc)
    text = (text or "").strip()

    # HH:MM 또는 HH:MM:SS → 오늘
    if re.match(r'^\d{1,2}:\d{2}(:\d{2})?$', text):
        return now

    # 상대 시각
    m = re.match(r'^(\d+)\s*(분|시간|일)\s*전$', text)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        delta = {"분": timedelta(minutes=n), "시간": timedelta(hours=n), "일": timedelta(days=n)}
        return now - delta[unit]

    # YYYY-MM-DD 또는 YYYY/MM/DD
    m = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})', text)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
        except ValueError:
            pass

    # YY.MM.DD 또는 YYYY.MM.DD
    m = re.match(r'^(\d{2,4})\.(\d{1,2})\.(\d{1,2})$', text)
    if m:
        try:
            y = int(m.group(1))
            if y < 100:
                y += 2000
            return datetime(y, int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
        except ValueError:
            pass

    # MM.DD → 올해
    m = re.match(r'^(\d{1,2})\.(\d{1,2})$', text)
    if m:
        try:
            return datetime(now.year, int(m.group(1)), int(m.group(2)), tzinfo=timezone.utc)
        except ValueError:
            pass

    return now


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

            date_el  = container.select_one("span.sds-comps-profile-info-item, span[class*='date'], span[class*='time']")
            pub_date = _parse_date(date_el.get_text() if date_el else "")
            post_id  = re.sub(r"[^a-zA-Z0-9]", "", href)[-20:] or title[:20]
            posts.append(RawPost(
                channel      = Channel.NAVER_NEWS,
                post_id      = post_id,
                url          = href,
                title        = title,
                body         = body,
                published_at = pub_date,
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
            date_el  = item.select_one("span.date, span.item-date, span[class*='date']")

            if not title_el:
                continue

            title    = _clean(title_el.get_text())
            href     = title_el.get("href", "")
            body     = _clean(desc_el.get_text()) if desc_el else title
            pub_date = _parse_date(date_el.get_text() if date_el else "")
            post_id  = re.sub(r"[^a-zA-Z0-9]", "", href)[-20:] or title[:20]

            if not title:
                continue

            posts.append(RawPost(
                channel      = Channel.DAUM_NEWS,
                post_id      = post_id,
                url          = href,
                title        = title,
                body         = body,
                published_at = pub_date,
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
            date_el   = item.select_one("span.date, span.item-date, span[class*='date']")
            pub_date  = _parse_date(date_el.get_text() if date_el else "")
            post_id   = re.sub(r"[^a-zA-Z0-9]", "", href)[-24:] or title[:20]

            if not title:
                continue

            posts.append(RawPost(
                channel      = Channel.DAUM_CAFE,
                post_id      = post_id,
                url          = href,
                title        = f"[{cafe_name}] {title}" if cafe_name else title,
                body         = body,
                published_at = pub_date,
            ))

        await asyncio.sleep(CRAWL_DELAY)
    except Exception as e:
        print(f"[DaumCafe] '{keyword}' 수집 실패: {e}")

    return posts



# ── 에펨코리아 (Playwright) ───────────────────────────────────────────────────

async def crawl_fmkorea(keyword: str) -> list[RawPost]:
    """
    에펨코리아 httpx 차단(430) → Playwright + channel=chrome 으로 우회
    URL: /?mid=search&search_target=title&search_keyword={keyword}
    셀렉터: h3.title a > span.ellipsis-target
    """
    url = f"https://www.fmkorea.com/?mid=search&search_target=title&search_keyword={quote(keyword)}"
    posts = []

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("[FMKorea] playwright 미설치")
        return posts

    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(channel=_PW_CHANNEL, headless=True)
            page = await browser.new_page(user_agent=HEADERS["User-Agent"])
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            h3_list = await page.query_selector_all("h3.title")
            for h3 in h3_list[:15]:
                a = await h3.query_selector("a")
                if not a:
                    continue

                span = await a.query_selector("span.ellipsis-target")
                if span:
                    title = _clean(await span.inner_text())
                else:
                    title = _clean(await a.inner_text())
                title = re.sub(r"\[\d+\]", "", title).strip()

                href = await a.get_attribute("href") or ""
                if href.startswith("/"):
                    href = "https://www.fmkorea.com" + href

                cmts_el  = await a.query_selector("span.comment_count")
                comments = _parse_int(await cmts_el.inner_text() if cmts_el else "")
                post_id  = (re.search(r"/(\d+)$", href) or [None, title[:12]])[1]

                if not title:
                    continue

                posts.append(RawPost(
                    channel      = Channel.FMKOREA,
                    post_id      = str(post_id),
                    url          = href,
                    title        = title,
                    body         = title,
                    published_at = datetime.now(timezone.utc),
                    comments     = comments,
                ))

            await browser.close()
        await asyncio.sleep(CRAWL_DELAY)
    except Exception as e:
        print(f"[FMKorea] '{keyword}' 수집 실패: {e}")

    return posts


# ── 클리앙 (정적 크롤링) ─────────────────────────────────────────────────────

async def crawl_clien(keyword: str) -> list[RawPost]:
    """
    클리앙 소비자 게시판 검색 — httpx로 충분 (서버사이드 렌더링)
    셀렉터: div.list_item:not(.blocked) > a.subject_fixed
    """
    url = f"https://www.clien.net/service/search?q={quote(keyword)}&sort=recency&boardName=cm_consumer"
    posts = []

    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        items = [i for i in soup.select("div.list_item") if "blocked" not in (i.get("class") or [])]

        for item in items[:15]:
            a = item.select_one("a.subject_fixed[data-role='list-title-text']")
            if not a:
                continue

            title = _clean(a.get("title") or a.get_text())
            href  = a.get("href", "")
            if href.startswith("/"):
                href = "https://www.clien.net" + href.split("?")[0]

            comments = _parse_int(item.get("data-comment-count", "0"))
            post_id  = (re.search(r"/(\d+)$", href) or [None, title[:12]])[1]
            date_el  = item.select_one("span.time, span.list_time, time")
            pub_date = _parse_date(
                date_el.get("datetime") or date_el.get_text() if date_el else ""
            )

            if not title:
                continue

            posts.append(RawPost(
                channel      = Channel.CLIEN,
                post_id      = str(post_id),
                url          = href,
                title        = title,
                body         = title,
                published_at = pub_date,
                comments     = comments,
            ))

        await asyncio.sleep(CRAWL_DELAY)
    except Exception as e:
        print(f"[Clien] '{keyword}' 수집 실패: {e}")

    return posts


# ── 뽐뿌 (httpx 정적 크롤링) ─────────────────────────────────────────────────

async def crawl_ppomppu(keyword: str) -> list[RawPost]:
    """
    뽐뿌 자유게시판 제목 검색 — httpx로 충분 (서버사이드 렌더링)
    URL: /zboard/zboard.php?id=freeboard&keyword={keyword}&keyfield=subject
    셀렉터: a.baseList-title[href*=id=freeboard] / span.baseList-c / td.baseList-views
    """
    url = f"https://www.ppomppu.co.kr/zboard/zboard.php?id=freeboard&keyword={quote(keyword)}&keyfield=subject"
    posts = []

    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True, timeout=15) as client:
            resp = await client.get(url)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        title_links = [a for a in soup.find_all("a", class_="baseList-title", href=True)
                       if "id=freeboard" in a.get("href", "")]

        for a in title_links[:15]:
            span = a.find("span")
            title = _clean(span.get_text() if span else a.get_text())
            if not title:
                continue

            raw_href = a.get("href", "")
            # keyword 파라미터 제거한 클린 URL
            no_match = re.search(r"no=(\d+)", raw_href)
            post_id  = no_match.group(1) if no_match else title[:12]
            href     = f"https://www.ppomppu.co.kr/zboard/view.php?id=freeboard&no={post_id}"

            row      = a.find_parent("tr")
            cmts_el  = a.find_parent("td").find("span", class_="baseList-c") if a.find_parent("td") else None
            views_el = row.find("td", class_="baseList-views") if row else None
            comments = _parse_int(cmts_el.get_text() if cmts_el else "")
            views    = _parse_int(views_el.get_text() if views_el else "")
            # 날짜 셀: baseList-space td 중 날짜/시간 패턴인 것을 찾음
            date_text = ""
            if row:
                for td in row.find_all("td", class_="baseList-space"):
                    t = td.get_text(strip=True)
                    if re.match(r'^\d{2}[.:]\d{2}', t):  # HH:MM 또는 YY.MM.DD
                        date_text = t
                        break
            pub_date = _parse_date(date_text)

            posts.append(RawPost(
                channel      = Channel.PPOMPPU,
                post_id      = post_id,
                url          = href,
                title        = title,
                body         = title,
                published_at = pub_date,
                views        = views,
                comments     = comments,
            ))

        await asyncio.sleep(CRAWL_DELAY)
    except Exception as e:
        print(f"[Ppomppu] '{keyword}' 수집 실패: {e}")

    return posts


# ── 통합 수집 함수 ────────────────────────────────────────────────────────────

CRAWLERS = [
    crawl_naver_news,
    crawl_daum_news,
    crawl_daum_cafe,
    crawl_ppomppu,
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
