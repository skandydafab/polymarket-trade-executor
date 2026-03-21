from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import re
from typing import Any, Mapping, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen


_MONTH_TOKENS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

_SLUG_STOPWORDS = {
    "the",
    "a",
    "an",
    "of",
    "on",
    "for",
    "in",
    "to",
    "and",
    "or",
    "with",
    "price",
    "above",
    "below",
    "over",
    "under",
    "inside",
    "outside",
    "between",
    "range",
    "will",
    "be",
    "is",
    "does",
    "if",
}

_TEXT_STOPWORDS = {
    "the",
    "a",
    "an",
    "of",
    "on",
    "for",
    "in",
    "to",
    "and",
    "or",
    "with",
    "will",
    "be",
    "is",
    "does",
    "if",
    "at",
    "by",
    "than",
    "close",
    "above",
    "below",
    "between",
    "inside",
    "outside",
}

_ASSET_ALIASES = {
    "bitcoin": "btc",
    "btc": "btc",
    "ethereum": "eth",
    "ether": "eth",
    "eth": "eth",
    "solana": "sol",
    "sol": "sol",
    "dogecoin": "doge",
    "doge": "doge",
}


@dataclass(frozen=True, slots=True)
class SlugResolutionConfig:
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    timeout_ms: int = 5000
    include_only_active_tradable: bool = True
    max_concurrency: int = 8


@dataclass(frozen=True, slots=True)
class SlugMarket:
    slug: str
    market_id: str
    question: str
    token_ids: tuple[str, ...]
    active: bool
    accepting_orders: bool
    closed: bool
    yes_price_cents: int | None
    no_price_cents: int | None = None
    yes_token_id: str | None = None
    no_token_id: str | None = None


@dataclass(frozen=True, slots=True)
class StructuralArbCandidate:
    slug_left: str
    slug_right: str
    left_market: SlugMarket
    right_market: SlugMarket
    similarity: float
    match_mode: str = "text"
    lower_strike_market: SlugMarket | None = None
    upper_strike_market: SlugMarket | None = None
    range_market: SlugMarket | None = None
    strike_low: int | None = None
    strike_high: int | None = None
    boundary_relation: str | None = None


@dataclass(frozen=True, slots=True)
class SlugSubscriptionPlan:
    slugs: tuple[str, ...]
    explicit_pairs: tuple[tuple[str, str], ...]
    auto_pair: bool
    match_mode: str


@dataclass(frozen=True, slots=True)
class _ParsedMarketSpec:
    asset_key: str
    date_key: str
    relation: str
    strikes_x100: tuple[int, ...]


def fetch_active_markets_for_slugs(
    slugs: Sequence[str],
    config: SlugResolutionConfig,
) -> dict[str, tuple[SlugMarket, ...]]:
    by_slug: dict[str, tuple[SlugMarket, ...]] = {}
    cleaned: list[str] = []
    seen: set[str] = set()
    for raw_slug in slugs:
        slug = raw_slug.strip()
        if not slug or slug in seen:
            continue
        seen.add(slug)
        cleaned.append(slug)

    if not cleaned:
        return by_slug

    worker_count = max(1, min(config.max_concurrency, len(cleaned)))
    if worker_count == 1:
        for slug in cleaned:
            try:
                by_slug[slug] = fetch_markets_for_slug(slug, config)
            except Exception:
                # Skip stale or invalid slugs so one failure does not abort the full batch.
                continue
        return by_slug

    resolved: dict[str, tuple[SlugMarket, ...]] = {}
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="slug-fetch") as pool:
        future_to_slug = {
            pool.submit(fetch_markets_for_slug, slug, config): slug
            for slug in cleaned
        }
        for future in as_completed(future_to_slug):
            slug = future_to_slug[future]
            try:
                resolved[slug] = future.result()
            except Exception:
                # Skip stale or invalid slugs so one failure does not abort the full batch.
                continue

    for slug in cleaned:
        markets = resolved.get(slug)
        if markets is not None:
            by_slug[slug] = markets

    return by_slug


def fetch_markets_for_slug(
    slug: str,
    config: SlugResolutionConfig,
) -> tuple[SlugMarket, ...]:
    payload = _request_markets_payload(slug, config)
    raw_markets = _extract_market_objects(payload)

    results: list[SlugMarket] = []
    seen_market_ids: set[str] = set()
    for raw in raw_markets:
        market = _to_slug_market(slug, raw)
        if market is None:
            continue
        if config.include_only_active_tradable and not _is_active_tradable(market):
            continue
        if market.market_id in seen_market_ids:
            continue
        seen_market_ids.add(market.market_id)
        results.append(market)

    return tuple(results)


def parse_slug_pairs(raw_value: str | None) -> tuple[tuple[str, str], ...]:
    if raw_value is None:
        return tuple()

    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for chunk in raw_value.split(","):
        text = chunk.strip()
        if not text:
            continue
        if "|" not in text:
            continue
        left_raw, right_raw = text.split("|", 1)
        left = left_raw.strip()
        right = right_raw.strip()
        if not left or not right:
            continue
        pair = (left, right)
        if pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)

    return tuple(pairs)


def build_slug_pairs(plan: SlugSubscriptionPlan) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = list(plan.explicit_pairs)
    seen = set(pairs)

    if plan.auto_pair:
        for pair in auto_pair_slugs(plan.slugs):
            if pair in seen:
                continue
            seen.add(pair)
            pairs.append(pair)

    return tuple(pairs)


def auto_pair_slugs(slugs: Sequence[str]) -> tuple[tuple[str, str], ...]:
    cleaned = [item.strip() for item in slugs if item.strip()]
    if len(cleaned) < 2:
        return tuple()

    grouped: dict[tuple[str, str], list[str]] = {}
    for slug in cleaned:
        key = (_slug_theme_key(slug), _slug_date_key(slug))
        grouped.setdefault(key, []).append(slug)

    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for group_slugs in grouped.values():
        if len(group_slugs) < 2:
            continue

        between_slugs = [item for item in group_slugs if "between" in item.lower()]
        other_slugs = [item for item in group_slugs if item not in between_slugs]

        if between_slugs and other_slugs:
            for left in sorted(other_slugs):
                for right in sorted(between_slugs):
                    pair = (left, right)
                    if pair in seen:
                        continue
                    seen.add(pair)
                    pairs.append(pair)
            continue

        sorted_group = sorted(group_slugs)
        for index in range(0, len(sorted_group) - 1, 2):
            pair = (sorted_group[index], sorted_group[index + 1])
            if pair in seen:
                continue
            seen.add(pair)
            pairs.append(pair)

    if not pairs and len(cleaned) == 2:
        return ((cleaned[0], cleaned[1]),)
    return tuple(pairs)


def build_structural_arb_candidates(
    pairs: Sequence[tuple[str, str]],
    markets_by_slug: Mapping[str, Sequence[SlugMarket]],
    *,
    match_mode: str = "strict",
    min_similarity: float = 0.40,
) -> tuple[StructuralArbCandidate, ...]:
    normalized_mode = (match_mode or "text").strip().lower()
    if normalized_mode == "strict":
        return _build_strict_two_strike_candidates(
            pairs=pairs,
            markets_by_slug=markets_by_slug,
        )

    candidates: list[StructuralArbCandidate] = []
    seen: set[tuple[str, str]] = set()

    for slug_left, slug_right in pairs:
        left_markets = tuple(markets_by_slug.get(slug_left, tuple()))
        right_markets = tuple(markets_by_slug.get(slug_right, tuple()))
        if not left_markets or not right_markets:
            continue

        for left in left_markets:
            for right in right_markets:
                market_pair_key = (left.market_id, right.market_id)
                if market_pair_key in seen:
                    continue

                if normalized_mode == "cross":
                    similarity = 1.0
                else:
                    similarity = _label_similarity(left.question, right.question)
                    if similarity < min_similarity:
                        continue

                seen.add(market_pair_key)
                candidates.append(
                    StructuralArbCandidate(
                        slug_left=slug_left,
                        slug_right=slug_right,
                        left_market=left,
                        right_market=right,
                        similarity=similarity,
                        match_mode=normalized_mode,
                    )
                )

    candidates.sort(key=lambda item: item.similarity, reverse=True)
    return tuple(candidates)


def _build_strict_two_strike_candidates(
    *,
    pairs: Sequence[tuple[str, str]],
    markets_by_slug: Mapping[str, Sequence[SlugMarket]],
) -> tuple[StructuralArbCandidate, ...]:
    candidates: list[StructuralArbCandidate] = []
    seen_market_sets: set[tuple[str, str, str]] = set()

    for slug_left, slug_right in pairs:
        left_markets = tuple(markets_by_slug.get(slug_left, tuple()))
        right_markets = tuple(markets_by_slug.get(slug_right, tuple()))
        if not left_markets or not right_markets:
            continue

        combined_markets = left_markets + right_markets
        parsed_by_market_id: dict[str, _ParsedMarketSpec] = {}
        for market in combined_markets:
            parsed = _parse_market_spec(market.question)
            if parsed is None:
                continue
            parsed_by_market_id[market.market_id] = parsed

        if not parsed_by_market_id:
            continue

        range_markets: list[tuple[SlugMarket, _ParsedMarketSpec]] = []
        boundary_index: dict[tuple[str, str, str, int], list[SlugMarket]] = {}

        for market in combined_markets:
            parsed = parsed_by_market_id.get(market.market_id)
            if parsed is None:
                continue
            if parsed.relation == "between" and len(parsed.strikes_x100) == 2:
                range_markets.append((market, parsed))
                continue
            if parsed.relation not in {"above", "below"} or len(parsed.strikes_x100) != 1:
                continue

            strike = parsed.strikes_x100[0]
            key = (parsed.asset_key, parsed.date_key, parsed.relation, strike)
            boundary_index.setdefault(key, []).append(market)

        if not range_markets:
            continue

        for range_market, range_spec in range_markets:
            low_strike, high_strike = sorted(range_spec.strikes_x100)
            for relation in ("above", "below"):
                low_key = (range_spec.asset_key, range_spec.date_key, relation, low_strike)
                high_key = (range_spec.asset_key, range_spec.date_key, relation, high_strike)
                lower_markets = sorted(boundary_index.get(low_key, tuple()), key=lambda item: item.market_id)
                upper_markets = sorted(boundary_index.get(high_key, tuple()), key=lambda item: item.market_id)

                if not lower_markets or not upper_markets:
                    continue

                for lower_market in lower_markets:
                    for upper_market in upper_markets:
                        if lower_market.market_id == upper_market.market_id:
                            continue
                        if lower_market.market_id == range_market.market_id:
                            continue
                        if upper_market.market_id == range_market.market_id:
                            continue

                        market_set = tuple(sorted((lower_market.market_id, upper_market.market_id, range_market.market_id)))
                        if market_set in seen_market_sets:
                            continue
                        seen_market_sets.add(market_set)

                        candidates.append(
                            StructuralArbCandidate(
                                slug_left=slug_left,
                                slug_right=slug_right,
                                left_market=lower_market,
                                right_market=range_market,
                                similarity=1.0,
                                match_mode="strict",
                                lower_strike_market=lower_market,
                                upper_strike_market=upper_market,
                                range_market=range_market,
                                strike_low=low_strike,
                                strike_high=high_strike,
                                boundary_relation=relation,
                            )
                        )

    candidates.sort(
        key=lambda item: (
            item.slug_left,
            item.slug_right,
            item.strike_low or 0,
            item.strike_high or 0,
            item.range_market.market_id if item.range_market is not None else "",
        )
    )
    return tuple(candidates)


def _parse_market_spec(question: str) -> _ParsedMarketSpec | None:
    lower_question = question.lower()
    numeric_values = _extract_numeric_values_x100(question)
    if not numeric_values:
        return None

    relation: str
    strikes: tuple[int, ...]
    if "between" in lower_question and len(numeric_values) >= 2:
        first, second = numeric_values[0], numeric_values[1]
        strikes = tuple(sorted((first, second)))
        relation = "between"
    elif re.search(r"\b(above|over|greater\s+than|at\s+least)\b", lower_question):
        strikes = (numeric_values[0],)
        relation = "above"
    elif re.search(r"\b(below|under|less\s+than|at\s+most)\b", lower_question):
        strikes = (numeric_values[0],)
        relation = "below"
    else:
        return None

    asset_key = _question_asset_key(question)
    date_key = _question_date_key(question)
    if asset_key == "unknown" or date_key == "unknown":
        return None

    return _ParsedMarketSpec(
        asset_key=asset_key,
        date_key=date_key,
        relation=relation,
        strikes_x100=strikes,
    )


def _extract_numeric_values_x100(text: str) -> tuple[int, ...]:
    values: list[int] = []
    seen: set[int] = set()
    for raw_value in re.findall(r"\$?\d[\d,]*(?:\.\d+)?", text):
        cleaned = raw_value.replace("$", "").replace(",", "").strip()
        if not cleaned:
            continue
        try:
            parsed = float(cleaned)
        except ValueError:
            continue
        if parsed <= 0:
            continue
        scaled = int(round(parsed * 100))
        if scaled in seen:
            continue
        seen.add(scaled)
        values.append(scaled)
    return tuple(values)


def _question_asset_key(question: str) -> str:
    tokens = re.findall(r"[a-z]+", question.lower())
    for token in tokens:
        canonical = _ASSET_ALIASES.get(token)
        if canonical is not None:
            return canonical

    for token in tokens:
        if token in _TEXT_STOPWORDS or token in _MONTH_TOKENS:
            continue
        return token

    return "unknown"


def _question_date_key(question: str) -> str:
    normalized = re.sub(r"[^a-z0-9\-_/ ]+", " ", question.lower())

    ymd = re.search(r"(20\d{2})[-_/](\d{1,2})[-_/](\d{1,2})", normalized)
    if ymd:
        year, month, day = ymd.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

    tokens = normalized.split()
    year = datetime.now(timezone.utc).year
    month = None
    day = None

    for index, token in enumerate(tokens):
        if token in _MONTH_TOKENS:
            month = _MONTH_TOKENS[token]
            if index + 1 < len(tokens):
                parsed_day = _parse_day_token(tokens[index + 1])
                if parsed_day is not None:
                    day = parsed_day
            if index + 2 < len(tokens) and re.fullmatch(r"20\d{2}", tokens[index + 2]):
                year = int(tokens[index + 2])
            continue

        if re.fullmatch(r"20\d{2}", token):
            year = int(token)

    if month is not None and day is not None:
        return f"{year:04d}-{month:02d}-{day:02d}"
    return "unknown"


def _parse_day_token(token: str) -> int | None:
    match = re.fullmatch(r"(\d{1,2})(?:st|nd|rd|th)?", token)
    if match is None:
        return None
    parsed = int(match.group(1))
    if 1 <= parsed <= 31:
        return parsed
    return None


def _request_markets_payload(slug: str, config: SlugResolutionConfig) -> object:
    base = config.gamma_api_url.rstrip("/")
    paths = (
        ("/markets", {"slug": slug, "limit": "500"}),
        ("/events", {"slug": slug}),
    )

    errors: list[str] = []
    for path, params in paths:
        url = f"{base}{path}?{urlencode(params)}"
        try:
            payload = _http_get_json(url, timeout_ms=config.timeout_ms)
            if _extract_market_objects(payload):
                return payload
            errors.append(f"{path} returned no market objects")
            continue
        except Exception as exc:
            errors.append(str(exc))
            continue

    joined_errors = "; ".join(errors) if errors else "unknown error"
    raise OSError(f"failed to resolve slug={slug}: {joined_errors}")


def _http_get_json(url: str, *, timeout_ms: int) -> object:
    timeout_seconds = max(0.1, timeout_ms / 1000.0)
    request = Request(url, method="GET", headers={"Accept": "application/json", "User-Agent": "polyexecutor/1.0"})
    with urlopen(request, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8", errors="replace")
    return json.loads(body)


def _extract_market_objects(payload: object) -> tuple[Mapping[str, Any], ...]:
    extracted: list[Mapping[str, Any]] = []

    def walk(value: object) -> None:
        if isinstance(value, Mapping):
            if _looks_like_market(value):
                extracted.append(value)
            for nested in value.values():
                walk(nested)
            return
        if isinstance(value, list):
            for nested in value:
                walk(nested)

    walk(payload)
    return tuple(extracted)


def _looks_like_market(value: Mapping[str, Any]) -> bool:
    has_market_id = any(key in value for key in ("conditionId", "condition_id", "id", "market_id"))
    has_question = any(key in value for key in ("question", "title", "name"))
    has_tokens = any(key in value for key in ("clobTokenIds", "tokenIds", "tokens"))
    return has_market_id and (has_question or has_tokens)


def _to_slug_market(slug: str, raw: Mapping[str, Any]) -> SlugMarket | None:
    market_id = _as_str(
        raw.get("conditionId")
        or raw.get("condition_id")
        or raw.get("market_id")
        or raw.get("id")
    )
    if market_id is None:
        return None

    question = _as_str(raw.get("question") or raw.get("title") or raw.get("name")) or market_id
    token_payload = raw.get("clobTokenIds") or raw.get("tokenIds") or raw.get("tokens")
    token_ids = _parse_token_ids(token_payload)
    yes_token_id, no_token_id = _extract_outcome_token_ids(raw, token_ids)

    active = _as_bool(raw.get("active"), default=True)
    accepting_orders = _as_bool(
        raw.get("accepting_orders")
        or raw.get("acceptingOrders")
        or raw.get("enable_order_book"),
        default=True,
    )
    closed = _as_bool(raw.get("closed") or raw.get("isClosed") or raw.get("resolved"), default=False)
    yes_price_cents, no_price_cents = _extract_yes_no_prices(raw)

    if yes_price_cents is None and no_price_cents is not None:
        yes_price_cents = _complement_cents(no_price_cents)
    if no_price_cents is None and yes_price_cents is not None:
        no_price_cents = _complement_cents(yes_price_cents)

    if yes_token_id is None and token_ids:
        yes_token_id = token_ids[0]
    if no_token_id is None and len(token_ids) >= 2:
        no_token_id = token_ids[1]

    return SlugMarket(
        slug=slug,
        market_id=market_id,
        question=question,
        token_ids=token_ids,
        active=active,
        accepting_orders=accepting_orders,
        closed=closed,
        yes_price_cents=yes_price_cents,
        no_price_cents=no_price_cents,
        yes_token_id=yes_token_id,
        no_token_id=no_token_id,
    )


def _is_active_tradable(market: SlugMarket) -> bool:
    return market.active and market.accepting_orders and not market.closed


def _parse_token_ids(raw: object) -> tuple[str, ...]:
    if raw is None:
        return tuple()
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return tuple()
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                return _parse_token_ids(parsed)
            except Exception:
                return tuple(item.strip() for item in text.split(",") if item.strip())
        return tuple(item.strip() for item in text.split(",") if item.strip())
    if isinstance(raw, list):
        token_ids: list[str] = []
        for item in raw:
            if isinstance(item, Mapping):
                token = _as_str(item.get("token_id") or item.get("tokenId") or item.get("id") or item.get("token"))
                if token:
                    token_ids.append(token)
                continue
            token = _as_str(item)
            if token:
                token_ids.append(token)
        return tuple(token_ids)
    if isinstance(raw, Mapping):
        values = raw.values()
        token_ids: list[str] = []
        for item in values:
            token = _as_str(item)
            if token:
                token_ids.append(token)
        return tuple(token_ids)
    return tuple()


def _extract_outcome_token_ids(
    raw: Mapping[str, Any],
    token_ids: Sequence[str],
) -> tuple[str | None, str | None]:
    yes_token_id = None
    no_token_id = None

    tokens = raw.get("tokens")
    if isinstance(tokens, list):
        for item in tokens:
            if not isinstance(item, Mapping):
                continue
            outcome_name = _as_str(item.get("outcome") or item.get("name") or item.get("label"))
            token_id = _as_str(item.get("token_id") or item.get("tokenId") or item.get("id") or item.get("token"))
            if outcome_name is None or token_id is None:
                continue
            normalized = outcome_name.strip().lower()
            if normalized == "yes" and yes_token_id is None:
                yes_token_id = token_id
            elif normalized == "no" and no_token_id is None:
                no_token_id = token_id

    outcomes = _coerce_list(raw.get("outcomes"))
    if outcomes is not None:
        for index, outcome in enumerate(outcomes):
            if index >= len(token_ids):
                break
            name = _as_str(outcome)
            if name is None:
                continue
            normalized = name.strip().lower()
            if normalized == "yes" and yes_token_id is None:
                yes_token_id = token_ids[index]
            elif normalized == "no" and no_token_id is None:
                no_token_id = token_ids[index]

    return yes_token_id, no_token_id


def _extract_yes_no_prices(raw: Mapping[str, Any]) -> tuple[int | None, int | None]:
    yes_price = None
    no_price = None

    candidate_values = (
        raw.get("price"),
        raw.get("yesPrice"),
        raw.get("bestAsk"),
        raw.get("mid"),
    )
    for value in candidate_values:
        cents = _to_cents(value)
        if cents is not None:
            yes_price = cents
            break

    no_candidates = (
        raw.get("noPrice"),
        raw.get("bestAskNo"),
        raw.get("bestNoAsk"),
    )
    for value in no_candidates:
        cents = _to_cents(value)
        if cents is not None:
            no_price = cents
            break

    outcomes = _coerce_list(raw.get("outcomes"))
    outcome_prices = _coerce_list(raw.get("outcomePrices"))
    if outcomes is not None and outcome_prices is not None:
        for index, outcome in enumerate(outcomes):
            name = _as_str(outcome)
            if name is None:
                continue
            if index >= len(outcome_prices):
                continue
            price = _to_cents(outcome_prices[index])
            if price is None:
                continue
            normalized = name.strip().lower()
            if normalized == "yes" and yes_price is None:
                yes_price = price
            elif normalized == "no" and no_price is None:
                no_price = price

    return yes_price, no_price


def _coerce_list(value: object) -> list[object] | None:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
            except Exception:
                return None
            if isinstance(parsed, list):
                return parsed
    return None


def _complement_cents(price_cents: int) -> int:
    return max(0, min(100, 100 - price_cents))


def _to_cents(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value) * 100
    if isinstance(value, int):
        if 0 <= value <= 100:
            return value
        return None
    if isinstance(value, float):
        if 0.0 <= value <= 1.0:
            return max(1, min(99, int(round(value * 100))))
        if 0.0 <= value <= 100.0:
            return max(1, min(99, int(round(value))))
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            if "." in text:
                return _to_cents(float(text))
            return _to_cents(int(text))
        except ValueError:
            return None
    return None


def _label_similarity(left: str, right: str) -> float:
    left_tokens = _text_tokens(left)
    right_tokens = _text_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    intersection = left_tokens.intersection(right_tokens)
    union = left_tokens.union(right_tokens)
    return len(intersection) / len(union)


def _slug_theme_key(slug: str) -> str:
    parts = re.split(r"[-_\s]+", slug.lower())
    tokens: list[str] = []
    for part in parts:
        token = part.strip()
        if not token:
            continue
        if token in _SLUG_STOPWORDS:
            continue
        if token in _MONTH_TOKENS:
            continue
        if token.isdigit():
            continue
        if re.fullmatch(r"\d{4}", token):
            continue
        tokens.append(token)
    if not tokens:
        return slug.lower()
    return "-".join(tokens)


def _slug_date_key(slug: str) -> str:
    lower_slug = slug.lower()
    ymd = re.search(r"(20\d{2})[-_](\d{1,2})[-_](\d{1,2})", lower_slug)
    if ymd:
        year, month, day = ymd.groups()
        return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"

    parts = re.split(r"[-_\s]+", lower_slug)
    month = None
    day = None
    year = datetime.now(timezone.utc).year
    for index, part in enumerate(parts):
        token = part.strip()
        if token in _MONTH_TOKENS:
            month = _MONTH_TOKENS[token]
            if index + 1 < len(parts) and parts[index + 1].isdigit():
                day_candidate = int(parts[index + 1])
                if 1 <= day_candidate <= 31:
                    day = day_candidate
            continue
        if re.fullmatch(r"20\d{2}", token):
            year = int(token)

    if month is not None and day is not None:
        return f"{year:04d}-{month:02d}-{day:02d}"

    return "unknown"


def _text_tokens(text: str) -> set[str]:
    cleaned = re.sub(r"[^a-z0-9]+", " ", text.lower())
    tokens: set[str] = set()
    for token in cleaned.split():
        if not token or token in _TEXT_STOPWORDS or len(token) <= 1:
            continue
        if token.isdigit():
            continue
        if token in _MONTH_TOKENS:
            continue
        tokens.add(token)
    return tokens


def _as_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return str(value)


def _as_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
    return default
