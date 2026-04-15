import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class Channel(str, Enum):
    FMKOREA   = "에펨코리아"
    RULIWEB   = "루리웹"
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
    fetched_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def uid(self) -> str:
        return hashlib.md5(f"{self.channel}:{self.post_id}".encode()).hexdigest()


# 감지할 브랜드와 검색 키워드 매핑
BRAND_KEYWORDS: dict[str, list[str]] = {
    "배민": ["배민", "배달의민족"],
    "쿠팡이츠":   ["쿠팡이츠"],
}

# 정규식 기반 언급 감지 — 띄어쓰기 변형 허용 (배달 의 민족, 쿠팡 이츠 등)
BRAND_MENTION_PATTERNS: dict[str, re.Pattern] = {
    "배달의민족": re.compile(r'배민|배달\s*의\s*민족'),
    "쿠팡이츠":   re.compile(r'쿠팡\s*이츠'),
}

def detect_brands(text: str) -> list[str]:
    found = []
    for brand, keywords in BRAND_KEYWORDS.items():
        pattern = BRAND_MENTION_PATTERNS.get(brand)
        if pattern:
            if pattern.search(text):
                found.append(brand)
        elif any(kw in text for kw in keywords):
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
