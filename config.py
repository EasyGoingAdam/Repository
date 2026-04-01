"""Configuration constants for Polymarket Volatility Monitor."""
import os

_HERE = os.path.dirname(os.path.abspath(__file__))

MARKET_ID = "1394299"
MARKET_SLUG = "us-forces-enter-iran-by-december-31-573-642-385-371-179-425-262"

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE = "https://clob.polymarket.com"

POLL_INTERVAL_SECONDS = 30
BOLLINGER_PERIOD = 20
BOLLINGER_STD_MULTIPLIER = 1.0  # Aggressive 1-sigma

DB_PATH = os.environ.get("DB_PATH", os.path.join(_HERE, "volatility.db"))
PORT = int(os.environ.get("PORT", "8888"))

# --- Momentum Indicators ---
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9
STOCH_K_PERIOD = 14
STOCH_D_PERIOD = 3
ROC_PERIOD = 12
WILLIAMS_R_PERIOD = 14

# --- Volatility Indicators ---
ATR_PERIOD = 14
KELTNER_EMA_PERIOD = 20
KELTNER_ATR_PERIOD = 10
KELTNER_MULTIPLIER = 2.0
HIST_VOL_WINDOW = 20
VOL_OF_VOL_WINDOW = 10

# --- Volume Indicators ---
OBV_LOOKBACK = 50
VOLUME_ROC_PERIOD = 12

# --- Mean Reversion ---
CCI_PERIOD = 20

# --- Pattern / Regime ---
ADX_PERIOD = 14
HURST_WINDOW = 100
SUPPORT_RESISTANCE_TOLERANCE = 0.005
SUPPORT_RESISTANCE_MIN_TOUCHES = 3

# --- Composite Signal ---
COMPOSITE_SIGNAL_THRESHOLD = 0.3
REGIME_TRENDING_ADX = 25.0
WEIGHT_MEAN_REVERSION = 0.30
WEIGHT_MOMENTUM = 0.25
WEIGHT_VOLATILITY = 0.20
WEIGHT_ORDER_FLOW = 0.15
WEIGHT_VOLUME = 0.10

# --- Risk ---
RISK_FREE_RATE = 0.05
RISK_COMPUTE_INTERVAL = 10  # every N polls
VAR_CONFIDENCE = 0.95
KELLY_FRACTION = 0.5  # half-Kelly for safety
