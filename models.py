"""Data models for Polymarket Analyzer."""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict
from datetime import datetime
import json


@dataclass
class Market:
    id: str
    question: str
    slug: str
    outcomes: List[str]
    outcome_prices: List[float]
    volume: float
    liquidity: float
    end_date: Optional[str]
    active: bool
    closed: bool
    description: str = ""
    category: str = ""
    volume_24hr: float = 0.0
    clob_token_ids: List[str] = field(default_factory=list)

    @property
    def yes_price(self) -> float:
        """Return YES price (implied probability 0-1)."""
        if self.outcomes and "Yes" in self.outcomes:
            idx = self.outcomes.index("Yes")
            return self.outcome_prices[idx] if idx < len(self.outcome_prices) else 0.5
        return self.outcome_prices[0] if self.outcome_prices else 0.5

    @property
    def no_price(self) -> float:
        return 1.0 - self.yes_price

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Prediction:
    market_id: str
    market_question: str
    timestamp: str
    yes_price_at_prediction: float
    predicted_yes_prob: float
    confidence: float          # 0-1
    reasoning: str
    signals: Dict              # key factors used
    outcome: Optional[str] = None   # "YES" / "NO" / None (pending)
    resolved_price: Optional[float] = None
    score: Optional[float] = None   # Brier score or similar

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Prediction":
        return cls(**d)


@dataclass
class VolatilityMetric:
    market_id: str
    window_minutes: int
    mean_price: float
    std_dev: float
    bollinger_upper: float
    bollinger_lower: float
    z_score: float
    current_price: float
    mean_reversion_signal: float
    price_range_high: float
    price_range_low: float
    timestamp: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class TradingSignal:
    id: str
    market_id: str
    market_question: str
    signal_type: str          # buy / sell / hold
    strength: float
    price_at_signal: float
    reasoning: str
    volatility_z_score: float
    mean_reversion_component: float
    momentum_component: float
    volume_component: float
    prediction_component: float
    timestamp: str = ""
    profit_loss: Optional[float] = None
    evaluated: bool = False

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PerformanceReport:
    total_predictions: int
    resolved: int
    pending: int
    avg_brier_score: float      # lower = better (0 perfect, 1 worst)
    calibration_error: float    # mean abs difference between predicted & actual
    accuracy: float             # when rounded to binary
    profit_loss_units: float    # hypothetical P&L if betting 1 unit each
    best_predictions: List[Dict]
    worst_predictions: List[Dict]

    def to_dict(self) -> dict:
        return asdict(self)
