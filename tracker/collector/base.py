import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum

# 모든 datetime은 KST(Asia/Seoul, UTC+9) 기준으로 통일.
# 원본 사이트들이 모두 KST이므로 변환 없이 직관적으로 다룬다.
KST = timezone(timedelta(hours=9))


def now_kst() -> datetime:
    """현재 KST aware datetime"""
    return datetime.now(KST)


class Channel(str, Enum):
    FMKOREA   = "에펨코리아"
    CLIEN     = "클리앙"
    PPOMPPU   = "뽐뿌"
    DAUM_CAFE = "다음카페"
    NAVER_NEWS = "네이버뉴스"
    DAUM_NEWS  = "다음뉴스"


@dataclass
class RawPost:
    channel: Channel
    post_id: str
    url: str
    title: str
    body: str
    published_at: datetime
    views: int = 0
    comments: int = 0
    likes: int = 0
    fetched_at: datetime = field(default_factory=now_kst)

    @property
    def uid(self) -> str:
        return hashlib.md5(f"{self.channel}:{self.post_id}".encode()).hexdigest()


# 감지할 브랜드와 정규식 기반 언급 패턴 — 띄어쓰기 변형 허용
# key 는 표준(canonical) 브랜드명. 이 값이 DB의 brand 컬럼·UI 표시에 그대로 사용됨.
BRAND_MENTION_PATTERNS: dict[str, re.Pattern] = {
    "배달의민족": re.compile(r'배민|배달\s*의\s*민족'),
    "쿠팡이츠":   re.compile(r'쿠팡\s*이츠'),
}

# 키워드 기반 fallback (정규식 미정의 브랜드 추가 시 사용)
BRAND_KEYWORDS: dict[str, list[str]] = {}


def detect_brands(text: str) -> list[str]:
    found = []
    for brand, pattern in BRAND_MENTION_PATTERNS.items():
        if pattern.search(text):
            found.append(brand)
    for brand, keywords in BRAND_KEYWORDS.items():
        if brand in found:
            continue
        if any(kw in text for kw in keywords):
            found.append(brand)
    return found


# 수집 대상 키워드 (모든 채널에서 이 키워드로 검색)
SEARCH_KEYWORDS = [
    "배민", "쿠팡이츠", "스타벅스", "맥도날드", "올리브영",
    "BBQ", "카카오뱅크", "다이소", "GS25", "이마트",
    "배달앱", "치킨값",
]

# 배민·쿠팡이츠 전용 키워드
PRIORITY_KEYWORDS = [
    "배민 논란", "배달의민족 논란", "배민 수수료",
    "쿠팡이츠 논란", "쿠팡이츠 라이더", "배달비",
]
