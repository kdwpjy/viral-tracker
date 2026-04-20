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

from .base import KST, Channel, RawPost, detect_brands, now_kst

# 로컬: channel="chrome" (시스템 Chrome), CI: channel="chromium" (설치된 Chromium)
_PW_CHANNEL = os.environ.get("PLAYWRIGHT_CHANNEL", "chrome")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

CRAWL_DELAY = 2.0  # 초 (사이트 부하 방지)


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _kst_date(year: int, month: int, day: int) -> datetime:
    """YYYY-MM-DD KST 자정 aware datetime"""
    return datetime(year, month, day, tzinfo=KST)


def _parse_date(text: str) -> datetime | None:
    """
    사이트별 날짜 텍스트(KST 가정)를 KST aware datetime 으로 반환.
    빈 문자열·미인식 포맷 → None 반환 (호출 측에서 sentinel 등으로 처리).
    지원 형식:
      "16:30"          → 오늘 now (절대시각 미상)
      "04.16"          → 올해 4월 16일 (KST 자정)
      "26.04.16"       → 2026-04-16 (KST 자정)
      "2026.04.16"     → 2026-04-16 (KST 자정)
      "2026-04-16"     → 2026-04-16 (KST 자정)
      "2026/04/16"     → 2026-04-16 (KST 자정)
      "26.04.19 00:16:08" → KST 결합형
      "N분/시간/일 전"  → 상대 시각 (KST now 기준)
    """
    text = (text or "").strip()
    if not text:
        return None

    now = now_kst()

    # HH:MM 또는 HH:MM:SS → 오늘 (실제 절대시각은 알 수 없으므로 now)
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
            return _kst_date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # YY.MM.DD HH:MM(:SS) 또는 YYYY.MM.DD HH:MM(:SS) 결합형 — 뽐뿌 td[title] 등
    m = re.match(r'^(\d{2,4})\.(\d{1,2})\.(\d{1,2})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?$', text)
    if m:
        try:
            y = int(m.group(1))
            if y < 100:
                y += 2000
            mo, d = int(m.group(2)), int(m.group(3))
            h, mi = int(m.group(4)), int(m.group(5))
            sec = int(m.group(6)) if m.group(6) else 0
            return datetime(y, mo, d, h, mi, sec, tzinfo=KST)
        except ValueError:
            pass

    # YY.MM.DD 또는 YYYY.MM.DD (날짜만)
    m = re.match(r'^(\d{2,4})\.(\d{1,2})\.(\d{1,2})$', text)
    if m:
        try:
            y = int(m.group(1))
            if y < 100:
                y += 2000
            return _kst_date(y, int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # YY/MM/DD 또는 YYYY/MM/DD (슬래시 구분 — 뽐뿌 time elem 텍스트)
    m = re.match(r'^(\d{2,4})/(\d{1,2})/(\d{1,2})$', text)
    if m:
        try:
            y = int(m.group(1))
            if y < 100:
                y += 2000
            return _kst_date(y, int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    # MM.DD → 올해 (KST 기준 '올해')
    m = re.match(r'^(\d{1,2})\.(\d{1,2})$', text)
    if m:
        try:
            return _kst_date(now.year, int(m.group(1)), int(m.group(2)))
        except ValueError:
            pass

    return None


def _parsed_or_drop(text: str) -> datetime:
    """
    _parse_date 결과를 안전한 published_at(KST aware)으로 변환.
    파싱 실패 시 30일 *과거* sentinel → pipeline의 7일 필터에서 자동 제외.
    """
    dt = _parse_date(text)
    if dt is not None:
        return dt
    return now_kst() - timedelta(days=30)


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
            pub_date = _parsed_or_drop(date_el.get_text() if date_el else "")
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
            pub_date = _parsed_or_drop(date_el.get_text() if date_el else "")
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


# 다음카페 크롤러 제거됨 — w=tot 통합검색은 다음뉴스와 동일한 셀렉터를 써서
# 사실상 뉴스 중복 수집만 하고 있었음. w=cafe 는 JS 리다이렉트로 차단되어 우회 불가.


# ── 에펨코리아 (Playwright) ───────────────────────────────────────────────────

async def crawl_fmkorea(keyword: str) -> list[RawPost]:
    """
    에펨코리아 httpx 차단(430) → Playwright + channel=chrome 으로 우회
    URL: /?mid=search&search_target=title&search_keyword={keyword}
    셀렉터:
      - 항목 컨테이너: div.li
      - 제목: h3.title a > span.ellipsis-target
      - 발행일(KST): span.regdate (예: "2026.03.23")
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

            li_list = await page.query_selector_all("div.li")
            for li in li_list[:15]:
                a = await li.query_selector("h3.title a")
                if not a:
                    continue

                span = await a.query_selector("span.ellipsis-target")
                title = _clean(await (span.inner_text() if span else a.inner_text()))
                title = re.sub(r"\[\d+\]", "", title).strip()
                if not title:
                    continue

                href = await a.get_attribute("href") or ""
                if href.startswith("/"):
                    href = "https://www.fmkorea.com" + href

                cmts_el  = await a.query_selector("span.comment_count")
                comments = _parse_int(await cmts_el.inner_text() if cmts_el else "")

                # span.regdate (검색 결과 항목 옆) → KST 날짜 → UTC 변환
                # 날짜 elem 없거나 파싱 실패 시 미래 sentinel로 7일 필터에서 자동 제외
                date_el = await li.query_selector("span.regdate")
                date_text = _clean(await date_el.inner_text()) if date_el else ""
                published_at = _parsed_or_drop(date_text)

                match = re.search(r"/(\d+)$", href)
                post_id = match.group(1) if match else title[:12]

                posts.append(RawPost(
                    channel      = Channel.FMKOREA,
                    post_id      = str(post_id),
                    url          = href,
                    title        = title,
                    body         = title,
                    published_at = published_at,
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
            pub_date = _parsed_or_drop(
                (date_el.get("datetime") or date_el.get_text()) if date_el else ""
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
            # 날짜 셀: td.baseList-space[title="YY.MM.DD HH:MM:SS"] 우선 사용 (가장 정확).
            # title 없으면 time.baseList-time 텍스트 (오늘=HH:MM:SS, 어제 이전=YY/MM/DD)
            date_text = ""
            if row:
                date_td = row.select_one("td.baseList-space[title]")
                if date_td:
                    title_attr = (date_td.get("title") or "").strip()
                    if re.match(r'^\d{2,4}\.\d{1,2}\.\d{1,2}', title_attr):
                        date_text = title_attr
                if not date_text:
                    time_el = row.select_one("time.baseList-time")
                    if time_el:
                        date_text = _clean(time_el.get_text())
            pub_date = _parsed_or_drop(date_text)

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


# ── 기사 페이지에서 실제 발행일 보정 ─────────────────────────────────────────

# HTTP fetch 시 스킵할 채널 (차단·JS 렌더링 필요)
# httpx fetch 스킵할 채널. 에펨은 httpx 차단(430) → Playwright 전용이라 유지.
# 뽐뿌는 httpx로 상세 페이지 접근 가능하므로 본문 보강을 위해 포함.
_SKIP_FETCH_CHANNELS = {Channel.FMKOREA.value}

import logging as _logging
_log = _logging.getLogger("crawlers")


def _parse_date_text(text: str) -> datetime | None:
    """
    기사 페이지에서 추출한 날짜 텍스트를 KST aware datetime 으로 반환.
    지원:
      ISO8601  "2026-04-16T06:41:00+09:00" / "2026-04-16T06:41:00Z"
                "2026-04-16T06:41+0900" / "2026-04-16 06:41:00"
      한국형   "2026. 4. 16. 06:41" / "2026. 4. 16."
    """
    if not text:
        return None
    text = text.strip()

    # 1) ISO8601 — Python 3.11+ fromisoformat 이 대부분의 변형 처리
    #    (Z, +09:00, +0900, naive, 공백 구분자, 마이크로초 등)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            # naive → KST 가정 (한국 사이트 메타태그 관행)
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST)
    except (ValueError, TypeError):
        pass

    # 2) 한국형 "2026. 4. 16. 06:41" — KST aware
    m = re.match(r'(\d{4})\.\s*(\d{1,2})\.\s*(\d{1,2})\.?\s*(?:(\d{1,2}):(\d{2}))?', text)
    if m:
        try:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            h  = int(m.group(4)) if m.group(4) else 0
            mi = int(m.group(5)) if m.group(5) else 0
            return datetime(y, mo, d, h, mi, tzinfo=KST)
        except ValueError:
            pass

    return None


def _jsonld_published(soup: BeautifulSoup) -> datetime | None:
    """schema.org JSON-LD 의 datePublished 추출 (한국 뉴스사이트 다수가 제공)"""
    import json as _json
    for s in soup.find_all("script", type="application/ld+json"):
        raw = s.string or s.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = _json.loads(raw)
        except Exception:
            continue
        # JSON-LD 는 dict 또는 list of dict
        candidates = data if isinstance(data, list) else [data]
        # @graph 구조도 평탄화
        flat = []
        for c in candidates:
            if isinstance(c, dict):
                flat.append(c)
                if isinstance(c.get("@graph"), list):
                    flat.extend(x for x in c["@graph"] if isinstance(x, dict))
        for c in flat:
            for key in ("datePublished", "dateCreated", "uploadDate"):
                v = c.get(key)
                if isinstance(v, str):
                    dt = _parse_date_text(v)
                    if dt:
                        return dt
    return None


# 본문 발행일임을 시사하는 한글/영문 prefix — 사이드바/푸터 위젯과 구별
_PUB_PREFIX_RE = re.compile(r'^(?:발행일|작성일|등록일|입력|기사입력|Published|PUBLISHED)')

# 임의 텍스트에서 datetime/date 패턴 추출 (한글 prefix 허용)
# - 표준 형식: 2026-04-18 20:04, 2026/04/18 20:04, 2026.04.18 20:04
# - 한국형:    2026. 4. 18. 20:04 (점 + 공백 + 단자리 가능, 다음뉴스 num_date 등)
_DATETIME_IN_TEXT_RE = re.compile(
    r'(\d{4}\.\s*\d{1,2}\.\s*\d{1,2}\.?(?:\s*\d{1,2}:\d{2}(?::\d{2})?)?'
    r'|\d{4}[-/]\d{1,2}[-/]\d{1,2}(?:[\sT]\d{1,2}:\d{2}(?::\d{2})?)?'
    r'|\d{4}\.\d{1,2}\.\d{1,2}(?:[\sT]\d{1,2}:\d{2}(?::\d{2})?)?'
    r')'
)


def _extract_date_from_soup(soup: BeautifulSoup) -> datetime | None:
    """HTML 파싱 결과에서 발행일 추출 (우선순위 순)"""
    # 1. JSON-LD datePublished — 가장 신뢰성 높음 (schema.org 표준)
    dt = _jsonld_published(soup)
    if dt:
        return dt

    # 2. OG/표준 메타태그 (property=)
    for prop in ("article:published_time", "article:modified_time",
                 "og:article:published_time"):
        el = soup.find("meta", property=prop)
        if el and el.get("content"):
            dt = _parse_date_text(el["content"])
            if dt:
                return dt

    # 3. meta name 태그
    for name in ("pubdate", "publishDate", "DATE", "article.published",
                 "article_date_original", "LastModifiedDate"):
        el = soup.find("meta", attrs={"name": name})
        if el and el.get("content"):
            dt = _parse_date_text(el["content"])
            if dt:
                return dt

    # 4. <time datetime="...">
    for el in soup.find_all("time", attrs={"datetime": True})[:3]:
        dt = _parse_date_text(el.get("datetime", ""))
        if dt:
            return dt

    # 5. 에펨코리아 (XE 플랫폼): <span class="date m_no">
    el = soup.select_one("span.date.m_no")
    if el:
        dt = _parse_date_text(el.get_text(strip=True))
        if dt:
            return dt

    # 6. 클리앙 등 article-date 류
    for sel in ("div.view-info span.view_time", "span.article-date",
                "em.date", "p.date"):
        el = soup.select_one(sel)
        if el:
            dt = _parse_date_text(el.get_text(strip=True))
            if dt:
                return dt

    # 7. class 이름에 'date' 포함 (광범위 fallback)
    #    - "발행일/입력/등록일" 같은 본문 prefix가 있으면 우선
    #    - 없으면 첫 elem (사이드바 위젯이 잡힐 위험은 있지만 prefix 우선이 막아줌)
    candidates = soup.select("[class*='date']")[:10]
    prefix_matches = [el for el in candidates
                      if _PUB_PREFIX_RE.match(el.get_text(strip=True))]
    for el in prefix_matches + candidates:
        text = el.get_text(strip=True)
        m = _DATETIME_IN_TEXT_RE.search(text)
        if m:
            dt = _parse_date_text(m.group(1))
            if dt:
                return dt

    return None


# ── 본문 추출 ────────────────────────────────────────────────────────────────

def _extract_body_text(soup: BeautifulSoup, url: str = "") -> str:
    """
    기사/포스트 페이지에서 본문 텍스트 추출 (요약 생성용).
    사이트별 specific selector 우선 + 일반 fallback.
    """
    host = url.lower()

    # 뽐뿌 자유게시판 상세
    if "ppomppu" in host:
        for sel in ("td.board-contents", "div.han", "div#quote", "div.board-contents"):
            el = soup.select_one(sel)
            if el:
                t = _clean(el.get_text(" "))
                if len(t) > 30:
                    return t[:800]

    # 클리앙
    if "clien.net" in host:
        for sel in ("div.post_article", "div.post-article", "div.post_content"):
            el = soup.select_one(sel)
            if el:
                t = _clean(el.get_text(" "))
                if len(t) > 30:
                    return t[:800]

    # 다음뉴스 (daum.net)
    if "daum.net" in host:
        for sel in ("section#harmonyContainer", "div#harmonyContainer",
                    "div.article_view", "div.article-view",
                    "article[role=article]"):
            el = soup.select_one(sel)
            if el:
                t = _clean(el.get_text(" "))
                if len(t) > 50:
                    return t[:800]

    # 일반 뉴스 (article 표준 / 흔한 클래스)
    for sel in ("article[itemprop='articleBody']",
                "div[itemprop='articleBody']",
                "article.article-body",
                "div.article-body", "div#articleBody", "div#article_body",
                "div.article-content", "div#article-content",
                "div.news-content", "div.content-article"):
        el = soup.select_one(sel)
        if el:
            t = _clean(el.get_text(" "))
            if len(t) > 80:
                return t[:800]

    # fallback: <article> 통째
    art = soup.find("article")
    if art:
        t = _clean(art.get_text(" "))
        if len(t) > 80:
            return t[:800]

    # 최종 fallback: <p> 체인
    ps = soup.find_all("p")
    if ps:
        t = _clean(" ".join(p.get_text() for p in ps[:8]))
        if len(t) > 80:
            return t[:800]

    return ""


async def fetch_actual_dates(posts: list[RawPost], concurrency: int = 5) -> None:
    """
    뉴스·커뮤니티 기사 URL을 직접 fetch해서 실제 발행일 + 본문 요약 보강.
    - httpx: 네이버뉴스·다음·클리앙·뽐뿌 (커뮤니티 포함 — body=title 이면 본문 보강)
    - skip: 에펨코리아 (httpx 차단. body=title 유지)
    """
    httpx_targets = [p for p in posts
                     if p.channel.value not in _SKIP_FETCH_CHANNELS
                     and p.url.startswith("http")]
    if not httpx_targets:
        return

    _log.info(f"  [fetch_actual_dates] httpx {len(httpx_targets)}건 시작...")
    sem = asyncio.Semaphore(concurrency)
    date_updated = 0
    body_updated = 0

    async def _fix(client: httpx.AsyncClient, post: RawPost) -> None:
        nonlocal date_updated, body_updated
        async with sem:
            try:
                resp = await client.get(post.url)
                if resp.status_code != 200:
                    return
                soup = BeautifulSoup(resp.text, "html.parser")
                # 1) 발행일 보정
                dt = _extract_date_from_soup(soup)
                if dt:
                    post.published_at = dt
                    date_updated += 1
                # 2) body 보강 — 현재 body가 title과 같거나 비었을 때만
                if not post.body or post.body.strip() == (post.title or "").strip():
                    body = _extract_body_text(soup, post.url)
                    if body and len(body) > 30:
                        post.body = body
                        body_updated += 1
            except Exception as e:
                _log.debug(f"  [fetch_actual_dates] skip {post.url[:60]}: {e}")

    # C2: AsyncClient를 한 번만 열어 커넥션 풀 재사용
    async with httpx.AsyncClient(
        headers=HEADERS, follow_redirects=True, timeout=10
    ) as client:
        await asyncio.gather(*[_fix(client, p) for p in httpx_targets])

    _log.info(
        f"  [fetch_actual_dates] 날짜 {date_updated}/{len(httpx_targets)}건, "
        f"본문 {body_updated}/{len(httpx_targets)}건 보강 완료"
    )


# ── 통합 수집 함수 ────────────────────────────────────────────────────────────

CRAWLERS = [
    crawl_naver_news,
    crawl_daum_news,
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
            # 각 post 에 이 호출을 트리거한 키워드 기록
            for p in posts:
                if keyword not in p.matched_keywords:
                    p.matched_keywords.append(keyword)
            all_posts.extend(posts)
        except Exception as e:
            print(f"[{crawl_fn.__name__}] 오류: {e}")
    return all_posts
