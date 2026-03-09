from __future__ import annotations

from typing import Callable

import pytest

from executor.planner import ExecutionPlanner, Opportunity, PlannerConfig, PricingSnapshot
from executor.risk import ExecutionRiskManager, RiskManagerConfig

from tests.helpers.factories import (
    make_default_planner_config,
    make_default_risk_config,
    make_opportunity,
    make_snapshots,
)


@pytest.fixture
def now_ns() -> int:
    return 1_700_000_000_000_000_000


@pytest.fixture
def planner_config() -> PlannerConfig:
    return make_default_planner_config()


@pytest.fixture
def risk_config() -> RiskManagerConfig:
    return make_default_risk_config()


@pytest.fixture
def planner(planner_config: PlannerConfig) -> ExecutionPlanner:
    return ExecutionPlanner(planner_config)


@pytest.fixture
def risk_manager(risk_config: RiskManagerConfig) -> ExecutionRiskManager:
    return ExecutionRiskManager(risk_config)


@pytest.fixture
def opportunity_factory(now_ns: int) -> Callable[..., Opportunity]:
    def _build(**overrides: object) -> Opportunity:
        return make_opportunity(now_ns=now_ns, **overrides)

    return _build


@pytest.fixture
def snapshot_factory(now_ns: int) -> Callable[..., dict[str, PricingSnapshot]]:
    def _build(**overrides: object) -> dict[str, PricingSnapshot]:
        return make_snapshots(now_ns=now_ns, **overrides)

    return _build
