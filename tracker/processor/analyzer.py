"""
감성 분류 (Gemma 4 E2B-it) + 바이럴 스코어 계산

## 백엔드 (환경변수 SENTIMENT_BACKEND 로 선택)
- 'transformers' (기본 auto, 로컬): HF transformers + FP16
- 'llamacpp'     (CI 권장): llama.cpp + GGUF Q4_K_M 양자화 (CPU 친화)
- 자동 (미지정): transformers 우선, 실패 시 llamacpp, 모두 실패 시 규칙 fallback
- DISABLE_LLM=1: LLM 완전 비활성화 → 규칙 기반
"""
import logging
import math
import os
import re
from dataclasses import dataclass, field
from datetime import datetime

from tracker.collector.base import KST, Channel, RawPost, detect_brands, BRAND_KEYWORDS, now_kst

_log = logging.getLogger("analyzer")


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
    # 이 글을 잡아낸 검색 키워드 (카드에 '🔍 배민 / 배달비' 형태로 표시)
    matched_keywords: list[str] = field(default_factory=list)


# ── 감성 분류 ─────────────────────────────────────────────────────────────────

# ── 모델 설정 ────────────────────────────────────────────────────────────────

_LLM_MODEL_ID   = os.environ.get("SENTIMENT_MODEL", "google/gemma-4-E2B-it")
_GGUF_REPO      = os.environ.get("SENTIMENT_GGUF_REPO", "unsloth/gemma-4-E2B-it-GGUF")
_GGUF_FILE      = os.environ.get("SENTIMENT_GGUF_FILE", "gemma-4-E2B-it-Q4_K_M.gguf")
_BACKEND_PREF   = os.environ.get("SENTIMENT_BACKEND", "auto").lower()

_llm_backend = None   # None=미시도, False=사용불가, ('transformers'|'llamacpp', obj)=성공
_VALID_LABELS = ("positive", "negative", "neutral")

_SENTIMENT_PROMPT = (
    "다음 한국어 글의 감성을 분류하세요. "
    "positive, negative, neutral 중 하나만 정확히 한 단어로 답하세요.\n\n"
    "제목: {title}\n내용: {body}\n답:"
)


def _try_transformers():
    try:
        import torch  # noqa: F401
        from transformers import pipeline as hf_pipeline
    except ImportError as e:
        _log.debug(f"transformers 미설치: {e}")
        return None
    try:
        _log.info(f"🤖 [transformers] LLM 로드 시작: {_LLM_MODEL_ID}")
        pipe = hf_pipeline(
            task="text-generation",
            model=_LLM_MODEL_ID,
            torch_dtype="auto",
            device_map="auto",
        )
        _log.info("✅ [transformers] LLM 로드 완료")
        return ("transformers", pipe)
    except Exception as e:
        _log.warning(f"[transformers] 로드 실패: {e}")
        return None


def _try_llamacpp():
    try:
        from llama_cpp import Llama
        from huggingface_hub import hf_hub_download
    except ImportError as e:
        _log.debug(f"llama-cpp-python/huggingface_hub 미설치: {e}")
        return None
    try:
        _log.info(f"🤖 [llamacpp] GGUF 준비: {_GGUF_REPO}/{_GGUF_FILE}")
        model_path = hf_hub_download(repo_id=_GGUF_REPO, filename=_GGUF_FILE)
        _log.info(f"🤖 [llamacpp] Llama 초기화...")
        llm = Llama(
            model_path=model_path,
            n_ctx=1024,
            n_threads=int(os.environ.get("LLAMACPP_THREADS", "4")),
            verbose=False,
        )
        _log.info("✅ [llamacpp] LLM 로드 완료")
        return ("llamacpp", llm)
    except Exception as e:
        _log.warning(f"[llamacpp] 로드 실패: {e}")
        return None


def _get_llm_backend():
    """최초 호출 시 1회만 backend 로드 (lazy). SENTIMENT_BACKEND 순서대로 시도."""
    global _llm_backend
    if _llm_backend is not None:
        return _llm_backend if _llm_backend else None
    if os.environ.get("DISABLE_LLM") == "1":
        _log.info("DISABLE_LLM=1 → 규칙 기반 감성 분류 사용")
        _llm_backend = False
        return None

    if _BACKEND_PREF == "llamacpp":
        _llm_backend = _try_llamacpp() or _try_transformers() or False
    elif _BACKEND_PREF == "transformers":
        _llm_backend = _try_transformers() or _try_llamacpp() or False
    else:  # auto
        _llm_backend = _try_transformers() or _try_llamacpp() or False

    if not _llm_backend:
        _log.info("LLM 백엔드 사용 불가 → 규칙 기반 fallback")
    return _llm_backend if _llm_backend else None


def _classify_transformers(pipe, title: str, body: str) -> str | None:
    prompt = _SENTIMENT_PROMPT.format(title=title[:200], body=body[:500])
    out = pipe([{"role": "user", "content": prompt}], max_new_tokens=8, do_sample=False)
    return _find_label(_extract_generated_text(out))


def _classify_llamacpp(llm, title: str, body: str) -> str | None:
    prompt = _SENTIMENT_PROMPT.format(title=title[:200], body=body[:500])
    resp = llm.create_chat_completion(
        messages=[{"role": "user", "content": prompt}],
        max_tokens=8,
        temperature=0.0,
    )
    return _find_label(resp["choices"][0]["message"]["content"])


def _find_label(text: str) -> str | None:
    t = (text or "").lower()
    for label in _VALID_LABELS:
        if label in t:
            return label
    return None


def classify_sentiment_llm(title: str, body: str) -> str | None:
    """LLM으로 감성 분류. 실패 시 None 반환 (호출 측에서 fallback)."""
    backend = _get_llm_backend()
    if not backend:
        return None
    kind, obj = backend
    try:
        if kind == "transformers":
            return _classify_transformers(obj, title, body)
        if kind == "llamacpp":
            return _classify_llamacpp(obj, title, body)
    except Exception as e:
        _log.debug(f"LLM 분류 실패 ({kind}, {title[:30]}): {e}")
    return None


def _extract_generated_text(out) -> str:
    """HF pipeline 출력에서 생성된 응답 텍스트만 추출."""
    if not out:
        return ""
    first = out[0] if isinstance(out, list) else out
    gen = first.get("generated_text") if isinstance(first, dict) else None
    if isinstance(gen, list):   # chat 형식 [{"role":"user",...}, {"role":"assistant","content": "..."}]
        for m in reversed(gen):
            if isinstance(m, dict) and m.get("role") == "assistant":
                return str(m.get("content", ""))
        return ""
    return str(gen or "")


def classify_sentiment_rules(text: str) -> str:
    """규칙 기반 감성 분류 (LLM fallback 또는 CI 모드)."""
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


def classify_sentiment(text: str, title: str = "") -> str:
    """
    호환용 엔트리포인트. title 인자가 없으면 text를 분리해 LLM에 넘김.
    LLM 결과가 유효하면 사용, 그렇지 않으면 규칙 기반.
    """
    if not title:
        # 기존 호출 호환 — 분리 불가능. 규칙 기반으로만.
        return classify_sentiment_rules(text)
    body = text[len(title):].strip() if text.startswith(title) else text
    llm_result = classify_sentiment_llm(title, body)
    return llm_result if llm_result in _VALID_LABELS else classify_sentiment_rules(text)


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


def generate_summary(title: str, body: str, sentiment: str = "") -> str:
    """
    제목+본문에서 첫 두 문장을 뽑아 요약.
    - body 가 title 과 같거나 title 로 시작하면 중복 제거
    - 전체 문장 리스트에서 동일 문장이 반복되면 dedup
    - (sentiment 인자는 호환용 — 이제 결과에 부착하지 않음)
    """
    title = (title or "").strip()
    body  = (body  or "").strip()

    # body 가 title 을 단순 복제한 경우 비움 (뽐뿌/클리앙 등 body=title 케이스)
    if body == title:
        body = ""
    # body 가 title 로 시작하면 title 부분 잘라냄
    elif body.startswith(title):
        body = body[len(title):].strip()

    full_text = (title + ". " + body).strip() if body else title
    sentences = re.split(r"[.。!?！？]\s*", full_text)
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]

    # 중복 문장 제거 (순서 유지)
    seen = set()
    unique = []
    for s in sentences:
        if s not in seen:
            seen.add(s)
            unique.append(s)

    if unique:
        summary = unique[0][:80]
        if len(unique) > 1:
            summary += " " + unique[1][:60]
        return summary
    return title[:100]


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
    sentiment    = classify_sentiment(full_text, title=post.title)
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
        matched_keywords = list(post.matched_keywords),
    )


GENERIC_BRAND_LABEL = "배달앱 일반"


def analyze_posts(posts: list[RawPost]) -> list[ProcessedIssue]:
    """
    글별 sentiment/tags/score 등을 계산해 ProcessedIssue 생성.
    brand 필드:
      - 배민·쿠팡이츠 둘 다 언급 → '배달의민족, 쿠팡이츠' (detect_brands 순서)
      - 하나만 언급 → 해당 브랜드
      - 둘 다 미언급 → '배달앱 일반'
    """
    issues = []
    for post in posts:
        brands = detect_brands(post.title + " " + post.body)
        brand = ", ".join(brands) if brands else GENERIC_BRAND_LABEL
        issues.append(analyze(post, brand))
    return issues
