"""
키워드 기반 감성 분류 + 바이럴 스코어 계산
LLM/API 없이 순수 규칙(rule-based)으로 동작
"""
import math
import re
from dataclasses import dataclass, field
from datetime import datetime

from tracker.collector.base import KST, Channel, RawPost, detect_brands, BRAND_KEYWORDS, now_kst


# ── 감성 키워드 사전 ──────────────────────────────────────────────────────────

NEGATIVE_KEYWORDS = [
    # 행동
    "불매", "환불", "고발", "신고", "항의", "경고", "취소", "탈퇴",
    # 판단
    "논란", "갑질", "사기", "최악", "욕", "비판", "문제", "실망",
    "화남", "짜증", "분노", "충격", "황당", "어이없", "기가 막",
    # 사건
    "사고", "피해", "사과", "해명", "인정", "위반", "불법", "폭로",
    "갈취", "착취", "압박", "강요", "꼼수", "횡포",
    # 배달앱 특화
    "수수료", "배달비 인상", "라이더 산재", "독점", "입점 강요",
    "점주 갑질", "수수료 인상", "라이더 착취",
]

POSITIVE_KEYWORDS = [
    # 감정
    "감동", "칭찬", "훌륭", "대박", "최고", "좋다", "좋아", "굿",
    "완벽", "친절", "착하다", "따뜻", "감사", "응원", "극찬",
    # 행동
    "인증", "구매", "재구매", "추천", "인기", "품절", "완판",
    "흥행", "대유행", "할인", "혜택", "이벤트",
    # 배달앱 특화
    "빠른배달", "가성비", "할인쿠폰", "무료배달",
]

# 이해관계자 키워드 (배달앱 특화)
STAKEHOLDER_KEYWORDS = {
    "소비자": ["소비자", "고객", "이용자", "유저", "배달비", "주문"],
    "점주":   ["점주", "사장님", "업주", "자영업", "식당", "입점", "수수료"],
    "라이더": ["라이더", "배달원", "배달기사", "산재", "배달부"],
    "직원":   ["직원", "내부고발", "블라인드", "본사", "야근", "인센티브"],
}


@dataclass
class ProcessedIssue:
    uid: str
    brand: str
    channel: str
    url: str
    title: str
    summary: str
    sentiment: str         # positive / negative / neutral
    tags: list[str]
    viral_score: float
    status: str            # Hot / Rising / Stable
    stakeholders: list[str]
    published_at: str
    processed_at: str = field(default_factory=lambda: now_kst().isoformat())
    views: int = 0
    comments: int = 0


# ── 감성 분류 ─────────────────────────────────────────────────────────────────

def classify_sentiment(text: str) -> str:
    text_lower = text.lower()

    neg_count = sum(1 for kw in NEGATIVE_KEYWORDS if kw in text_lower)
    pos_count = sum(1 for kw in POSITIVE_KEYWORDS if kw in text_lower)

    if neg_count > pos_count:
        return "negative"
    if pos_count > neg_count:
        return "positive"
    if neg_count == pos_count and neg_count > 0:
        return "negative"  # 양쪽 같으면 부정 우선 (부정이 더 바이럴됨)
    return "neutral"


def extract_tags(text: str) -> list[str]:
    tags = []
    checks = [
        ("불매운동",      ["불매"]),
        ("갑질논란",      ["갑질"]),
        ("감동마케팅",    ["감동", "칭찬", "친절"]),
        ("사과논란",      ["사과", "해명"]),
        ("가성비논란",    ["가성비", "가격", "인상"]),
        ("직원썰",        ["직원", "블라인드", "내부고발"]),
        ("광고역효과",    ["광고 사기", "광고랑 다르", "실물이"]),
        ("배달비논란",    ["배달비", "배달비 인상"]),
        ("수수료갑질",    ["수수료", "수수료 인상"]),
        ("라이더이슈",    ["라이더", "배달원", "산재"]),
        ("독점입점압박",  ["독점", "입점 강요", "강요"]),
        ("점주썰",        ["점주", "사장님", "자영업"]),
        ("품질논란",      ["품질", "위생", "이물질"]),
        ("가격인상",      ["가격 인상", "인상", "올랐"]),
        ("직원칭찬",      ["직원", "칭찬", "감동"]),
    ]
    for tag, keywords in checks:
        if any(kw in text for kw in keywords):
            tags.append(f"#{tag}")
    return tags[:5]  # 최대 5개


def detect_stakeholders(text: str) -> list[str]:
    found = []
    for role, keywords in STAKEHOLDER_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            found.append(role)
    return found or ["소비자"]


def generate_summary(title: str, body: str, sentiment: str) -> str:
    """제목+본문에서 첫 두 문장을 뽑아 요약 (LLM 없이)"""
    full_text = (title + " " + body).strip()
    # 문장 분리
    sentences = re.split(r"[.。!?！？]\s*", full_text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]

    sentiment_label = {
        "negative": "부정",
        "positive": "긍정",
        "neutral":  "중립",
    }.get(sentiment, "")

    if sentences:
        summary = sentences[0][:80]
        if len(sentences) > 1:
            summary += " " + sentences[1][:60]
        return summary + f" [{sentiment_label} 바이럴]"
    return title[:100] + f" [{sentiment_label} 바이럴]"


# ── 바이럴 스코어 계산 ────────────────────────────────────────────────────────

CHANNEL_WEIGHT: dict[str, float] = {
    "에펨코리아": 1.2,
    "클리앙":     0.9,
    "다음카페":   0.9,
    "네이버뉴스": 1.4,
    "다음뉴스":   1.1,
}

SENTIMENT_MULTIPLIER: dict[str, float] = {
    "negative": 1.3,
    "positive": 1.0,
    "neutral":  0.7,
}

def compute_viral_score(post: RawPost, sentiment: str) -> float:
    # 반응 지표 (로그 스케일)
    engagement = (
        math.log1p(post.views)    * 0.5 +
        math.log1p(post.comments) * 2.0 +
        math.log1p(post.likes)    * 1.0
    )
    engagement_score = min(engagement * 3, 60.0)  # max 60점

    # 시간 점수 (최신일수록 높음, KST 기준)
    now = now_kst()
    pub = post.published_at
    if pub.tzinfo is None:
        # naive datetime은 KST로 가정 (크롤러가 모두 aware KST를 반환하지만 방어)
        pub = pub.replace(tzinfo=KST)
    hours = max(0.0, (now - pub.astimezone(KST)).total_seconds() / 3600)
    recency_score = 25.0 if hours <= 6 else 15.0 if hours <= 24 else 5.0 if hours <= 72 else 0.0

    # 키워드 밀도 보너스 (제목+본문에 감성 키워드가 많을수록)
    text = post.title + " " + post.body
    kw_count = (
        sum(1 for kw in NEGATIVE_KEYWORDS if kw in text) +
        sum(1 for kw in POSITIVE_KEYWORDS if kw in text)
    )
    kw_bonus = min(kw_count * 2, 15.0)  # max 15점

    raw = (engagement_score + recency_score + kw_bonus)
    raw *= CHANNEL_WEIGHT.get(post.channel.value, 1.0)
    raw *= SENTIMENT_MULTIPLIER.get(sentiment, 1.0)

    return round(min(raw, 100.0), 1)


def classify_status(score: float) -> str:
    if score >= 75:
        return "Hot"
    if score >= 50:
        return "Rising"
    return "Stable"


# ── 메인 분석 함수 ────────────────────────────────────────────────────────────

def analyze(post: RawPost, brand: str) -> ProcessedIssue:
    full_text = post.title + " " + post.body
    sentiment    = classify_sentiment(full_text)
    tags         = extract_tags(full_text)
    stakeholders = detect_stakeholders(full_text)
    summary      = generate_summary(post.title, post.body, sentiment)
    viral_score  = compute_viral_score(post, sentiment)
    status       = classify_status(viral_score)

    return ProcessedIssue(
        uid          = post.uid,
        brand        = brand,
        channel      = post.channel.value,
        url          = post.url,
        title        = post.title,
        summary      = summary,
        sentiment    = sentiment,
        tags         = tags,
        viral_score  = viral_score,
        status       = status,
        stakeholders = stakeholders,
        published_at = post.published_at.isoformat(),
        views        = post.views,
        comments     = post.comments,
    )


def analyze_posts(posts: list[RawPost]) -> list[ProcessedIssue]:
    issues = []
    for post in posts:
        brands = detect_brands(post.title + " " + post.body)
        if not brands:
            brands = ["기타"]
        # 브랜드별로 하나씩 이슈 생성
        issue = analyze(post, brands[0])
        issues.append(issue)
    return issues
