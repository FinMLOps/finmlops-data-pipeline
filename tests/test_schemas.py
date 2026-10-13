"""Unit tests for core schemas and data types."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scripts.helpers.schemas import (
    UTC,
    Article,
    ArticleScore,
    ScoredArticle,
    make_id,
    parse_dt,
    to_utc,
)


def test_to_utc():
    sample_time_utc_unaware = datetime(2026, 10, 12, 15, 59, 30)
    sample_time_utc_aware = to_utc(sample_time_utc_unaware)
    assert sample_time_utc_aware.tzinfo == UTC

    sample_time_already_utc_aware = datetime(
        2026, 10, 12, 15, 59, 30, tzinfo=timezone.utc
    )
    assert to_utc(sample_time_already_utc_aware) == sample_time_already_utc_aware


def test_parse_dt():
    CORRECT_OUTPUT = "2026-10-12T15:59:30+00:00"

    # Unix timestamp in seconds
    assert parse_dt(1791820770).isoformat() == CORRECT_OUTPUT
    assert parse_dt("1791820770").isoformat() == CORRECT_OUTPUT  # numeric string

    # Unix timestamp in milliseconds
    assert parse_dt(1791820770000).isoformat() == CORRECT_OUTPUT
    assert parse_dt("1791820770000").isoformat() == CORRECT_OUTPUT

    assert parse_dt("20261012T155930").isoformat() == CORRECT_OUTPUT
    assert parse_dt("2026-10-12T15:59:30Z").isoformat() == CORRECT_OUTPUT

    # Invalid timestamp
    with pytest.raises(ValueError, match="unrecognised timestamp"):
        parse_dt("not_a_valid_date")


def test_make_id():
    id1 = make_id("marketaux", "https://news.com/1", "ABC")
    id2 = make_id("marketaux", "https://news.com/1", "ABC")
    id3 = make_id("marketaux", "https://news.com/2", "XYZ")

    assert id1 == id2
    assert id1 != id3
    assert len(id1) == 16


def test_article_properties_and_serialization():
    dt = parse_dt("2026-10-12T15:59:30Z")
    article = Article(
        id="sample_id_123",
        source="marketaux",
        title="Tesla surges on robotaxi news",
        published=dt,
        url="https://news.com/tsla",
        summary="Elon Musk announces upcoming launch.",
        symbols=("TSLA",),
    )

    # .text joins title and summary
    assert (
        article.text
        == "Tesla surges on robotaxi news\n\nElon Musk announces upcoming launch."
    )

    # content digest produces 16-char fingerprint
    assert len(article.content_digest) == 16

    # to_dict and roundtrip
    d = article.to_dict()
    assert d["title"] == "Tesla surges on robotaxi news"
    assert d["symbols"] == ["TSLA"]

    reconstructed = Article.from_dict(d)
    assert reconstructed.id == article.id
    assert reconstructed.title == article.title
    assert reconstructed.symbols == article.symbols


def test_article_without_summary():
    dt = parse_dt("2026-10-12T15:59:30Z")
    article = Article(
        id="no_sum",
        source="finnhub",
        title="Headline Only",
        published=dt,
        summary=None,
    )
    # .text falls back to title alone when summary is None
    assert article.text == "Headline Only"


def test_article_from_dict():
    raw_data = {
        "title": "Apple Beats Q3 Earnings",
        "published": "2026-10-12T15:59:30Z",
        "source": "finnhub",
        "url": "https://news.com/aapl",
        "summary": "Revenue up 10%.",
        "symbols": ["AAPL", "aapl"],  # test uppercase normalization
    }
    article = Article.from_dict(raw_data)
    assert article.title == "Apple Beats Q3 Earnings"
    assert "AAPL" in article.symbols

    # Missing required field raises ValueError
    with pytest.raises(ValueError, match="article missing required field 'title'"):
        Article.from_dict({"published": "2026-10-12T15:59:30Z"})


def test_article_score_validation():
    # Valid score
    score = ArticleScore(score=0.8, confidence=0.9)
    assert score.score == 0.8
    assert score.confidence == 0.9

    # Score outside [-1.0, 1.0] raises ValueError
    with pytest.raises(ValueError, match="score must be in \\[-1, 1\\]"):
        ArticleScore(score=1.5)

    # Confidence outside [0.0, 1.0] raises ValueError
    with pytest.raises(ValueError, match="confidence must be in \\[0, 1\\]"):
        ArticleScore(score=0.5, confidence=-0.2)


def test_article_score_from_finbert():
    # Hugging Face style softmax probability distribution
    finbert_preds = [
        {"label": "positive", "score": 0.898},
        {"label": "neutral", "score": 0.082},
        {"label": "negative", "score": 0.020},
    ]
    score = ArticleScore.from_finbert(finbert_preds)

    assert round(score.score, 3) == 0.878  # pos (0.898) - neg (0.020)
    assert score.confidence == 0.898  # top probability
    assert score.prediction == "positive"  # winning class
    assert score.to_dict()["prediction"] == "positive"

    # Dictionary input
    score_dict = ArticleScore.from_finbert(
        {"negative": 0.95, "neutral": 0.03, "positive": 0.02}
    )
    assert round(score_dict.score, 2) == -0.93
    assert score_dict.prediction == "negative"


def test_scored_article_with_finbert():
    dt = parse_dt("2026-10-12T15:59:30Z")
    article = Article(
        id="a1",
        source="finnhub",
        title="Apple beats estimates",
        published=dt,
        symbols=("AAPL",),
    )
    score = ArticleScore.from_finbert(
        [
            {"label": "positive", "score": 0.85},
            {"label": "neutral", "score": 0.10},
            {"label": "negative", "score": 0.05},
        ]
    )
    scored = ScoredArticle(article=article, score=score)

    out = scored.to_dict()
    assert out["title"] == "Apple beats estimates"
    assert round(out["score"], 2) == 0.80
    assert out["confidence"] == 0.85
    assert out["prediction"] == "positive"

    # Reconstruct from dictionary roundtrip
    reconstructed = ScoredArticle.from_dict(out)
    assert reconstructed.article.title == "Apple beats estimates"
    assert reconstructed.score.prediction == "positive"
