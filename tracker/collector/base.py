from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import hashlib


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
    "배달의민족": ["배민", "배달의민족", "우아한형제들"],
    "쿠팡이츠":   ["쿠팡이츠"],
    "BBQ":       ["BBQ", "비비큐"],
    "스타벅스":   ["스타벅스"],
    "맥도날드":   ["맥도날드", "맥날"],
    "올리브영":   ["올리브영", "올영"],
    "카카오뱅크": ["카카오뱅크", "카뱅"],
    "다이소":     ["다이소"],
    "GS25":      ["GS25"],
    "이마트":     ["이마트"],
    "쿠팡":       ["쿠팡"],
    "네이버":     ["네이버"],
    "카카오":     ["카카오"],
}

def detect_brands(text: str) -> list[str]:
    found = []
    for brand, keywords in BRAND_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            found.append(brand)
    return found


# 수집 대상 키워드 (모든 채널에서 이 키워드로 검색)
SEARCH_KEYWORDS = [
    "배민", "쿠팡이츠", "스타벅스", "맥도날드", "올리브영",
    "BBQ", "카카오뱅크", "다이소", "GS25", "이마트",
    "배달앱", "치킨값", "편의점 논란",
]

# 배민·쿠팡이츠 전용 키워드
PRIORITY_KEYWORDS = [
    "배민 논란", "배달의민족 논란", "배민 수수료",
    "쿠팡이츠 논란", "쿠팡이츠 라이더", "배달비",
]
