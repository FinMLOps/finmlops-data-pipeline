"""Core data schemas for financial news ingestion, validation, and sentiment scoring."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

UTC = timezone.utc
_EPOCH_MS_THRESHOLD = 1e11


def to_utc(dt: datetime) -> datetime:
    """Ensure datetime is timezone-aware in UTC."""
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def parse_dt(value: Any) -> datetime:
    """Parse timestamps (Finnhub's unix seconds, Marketaux's ISO 8601) into aware UTC."""
    if isinstance(value, datetime):
        return to_utc(value)

    if isinstance(value, (int, float)):
        seconds = value / 1000.0 if value > _EPOCH_MS_THRESHOLD else float(value)
        return datetime.fromtimestamp(seconds, tz=UTC)

    text = str(value).strip()

    if text.isdigit():
        val = int(text)
        seconds = val / 1000.0 if val > _EPOCH_MS_THRESHOLD else float(val)
        return datetime.fromtimestamp(seconds, tz=UTC)

    # compact form YYYYMMDDTHHMMSS (e.g. 20241108T012400)
    if len(text) == 15 and text[8] == "T" and text[:8].isdigit() and text[9:].isdigit():
        return datetime(
            int(text[:4]),
            int(text[4:6]),
            int(text[6:8]),
            int(text[9:11]),
            int(text[11:13]),
            int(text[13:15]),
            tzinfo=UTC,
        )

    # standard ISO 8601 (Marketaux)
    try:
        return to_utc(datetime.fromisoformat(text))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"unrecognised timestamp: {value!r}") from exc


def make_id(*parts: str) -> str:
    """Deterministic short id from a handful of strings (source, url, title...)."""
    digest = hashlib.sha1(
        "\x1f".join(parts).encode("utf-8"),
        usedforsecurity=False,
    ).hexdigest()
    return digest[:16]


@dataclass(frozen=True, slots=True)
class Article:
    """One financial news item as returned by an API source."""

    id: str
    source: str
    title: str
    published: datetime
    url: str | None = None
    summary: str | None = None
    symbols: tuple[str, ...] = ()
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @property
    def text(self) -> str:
        """Title plus summary: the input text scored by FinBERT."""
        return f"{self.title}\n\n{self.summary}" if self.summary else self.title

    @property
    def content_digest(self) -> str:
        """Content fingerprint for deduplication and cache validation."""
        return make_id(
            self.title,
            self.summary or "",
            ",".join(self.symbols),
            self.published.isoformat(),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "title": self.title,
            "published": self.published.isoformat(),
            "url": self.url,
            "summary": self.summary,
            "symbols": list(self.symbols),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Article:
        try:
            title = str(data["title"])
            published = parse_dt(data["published"])
        except KeyError as exc:
            raise ValueError(f"article missing required field {exc.args[0]!r}") from exc
        except (TypeError, ValueError) as exc:
            raise ValueError(f"article {data.get('id') or title!r}: {exc}") from exc

        source = str(data.get("source") or "external")
        url = data.get("url") or None
        article_id = data.get("id") or make_id(
            source, url or f"{title}|{published.isoformat()}"
        )

        symbols_raw = data.get("symbols")
        if not symbols_raw:
            symbols = ()
        elif isinstance(symbols_raw, str):
            symbols = (symbols_raw.strip().upper(),) if symbols_raw.strip() else ()
        else:
            symbols = tuple(str(sym).strip().upper() for sym in symbols_raw if sym)

        raw_summary = data.get("summary")
        summary = raw_summary.strip() or None if isinstance(raw_summary, str) else None

        raw_payload = data.get("raw")
        raw_dict = dict(raw_payload) if raw_payload else {}

        return cls(
            id=str(article_id),
            source=source,
            title=title.strip(),
            published=published,
            url=url,
            summary=summary,
            symbols=symbols,
            raw=raw_dict,
        )


@dataclass(frozen=True, slots=True)
class ArticleScore:
    """The sentiment prediction from FinBERT on one article."""

    score: float  # [-1.0, 1.0]
    confidence: float = 1.0  # [0.0, 1.0]
    labels: Mapping[str, Any] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        if not -1.0 <= self.score <= 1.0:
            raise ValueError(f"score must be in [-1, 1], got {self.score!r}")
        if not 0.0 <= self.confidence <= 1.0:
            raise ValueError(f"confidence must be in [0, 1], got {self.confidence!r}")

    @property
    def prediction(self) -> str | None:
        """The top predicted class label (e.g. 'positive', 'neutral', 'negative')."""
        val = self.labels.get("prediction")
        return str(val) if val is not None else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "score": self.score,
            "confidence": self.confidence,
            "prediction": self.prediction,
            "labels": dict(self.labels),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ArticleScore:
        labels = dict(data.get("labels") or {})
        if "prediction" in data and "prediction" not in labels:
            labels["prediction"] = data["prediction"]
        return cls(
            score=float(data["score"]),
            confidence=float(data.get("confidence", 1.0)),
            labels=labels,
        )

    @classmethod
    def from_finbert(cls, predictions: list[Mapping[str, Any]] | Mapping[str, float]) -> ArticleScore:
        """Construct an ArticleScore directly from Hugging Face FinBERT output.

        Accepts either:
          - A list of dicts: [{'label': 'positive', 'score': 0.898}, ...]
          - A dictionary: {'positive': 0.898, 'neutral': 0.082, 'negative': 0.020}
        """
        if isinstance(predictions, list):
            prob_map = {str(item["label"]).lower(): float(item["score"]) for item in predictions}
        elif isinstance(predictions, Mapping):
            prob_map = {str(k).lower(): float(v) for k, v in predictions.items()}
        else:
            raise TypeError(f"expected list or mapping of predictions, got {type(predictions).__name__}")

        pos = prob_map.get("positive", 0.0)
        neg = prob_map.get("negative", 0.0)
        sentiment_score = pos - neg

        top_label = max(prob_map, key=prob_map.get) if prob_map else "neutral"
        confidence = prob_map.get(top_label, 1.0)

        return cls(
            score=sentiment_score,
            confidence=confidence,
            labels={
                "prediction": top_label,
                "probabilities": prob_map,
            },
        )


@dataclass(frozen=True, slots=True)
class ScoredArticle:
    article: Article
    score: ArticleScore

    def to_dict(self) -> dict[str, Any]:
        return {**self.article.to_dict(), **self.score.to_dict()}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ScoredArticle:
        return cls(article=Article.from_dict(data), score=ArticleScore.from_dict(data))


@dataclass(frozen=True, slots=True)
class ScoreFailure:
    article_id: str
    source: str
    error_type: str
    message: str
    attempts: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "article_id": self.article_id,
            "source": self.source,
            "error_type": self.error_type,
            "message": self.message,
            "attempts": self.attempts,
        }


class RunStatus:
    NO_ARTICLES = "no_articles"
    ALL_FAILED = "all_failed"
    PARTIAL = "partial"
    OK = "ok"

    ORDER = (NO_ARTICLES, ALL_FAILED, PARTIAL, OK)
    FAILED = frozenset({NO_ARTICLES, ALL_FAILED})


@dataclass(frozen=True, slots=True)
class RunCounts:
    fetched: int = 0
    deduplicated: int = 0
    filtered: int = 0
    submitted: int = 0
    scored: int = 0
    failed: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "fetched": self.fetched,
            "deduplicated": self.deduplicated,
            "filtered": self.filtered,
            "submitted": self.submitted,
            "scored": self.scored,
            "failed": self.failed,
        }


@dataclass(slots=True)
class ScoreResult:
    query: str
    since: datetime
    until: datetime
    score: float
    confidence: float
    n_articles: int
    by_source: dict[str, float] = field(default_factory=dict)
    articles: list[ScoredArticle] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    status: str = RunStatus.OK
    counts: RunCounts = field(default_factory=RunCounts)
    failures: list[ScoreFailure] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status == RunStatus.OK

    def to_dict(self, *, include_articles: bool = True) -> dict[str, Any]:
        data: dict[str, Any] = {
            "query": self.query,
            "since": self.since.isoformat(),
            "until": self.until.isoformat(),
            "score": self.score,
            "confidence": self.confidence,
            "n_articles": self.n_articles,
            "by_source": dict(self.by_source),
            "errors": list(self.errors),
            "status": self.status,
            "counts": self.counts.to_dict(),
            "failures": [f.to_dict() for f in self.failures],
        }
        if include_articles:
            data["articles"] = [a.to_dict() for a in self.articles]
        return data


def utcnow() -> datetime:
    return datetime.now(tz=UTC)
