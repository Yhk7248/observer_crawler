from dataclasses import dataclass


@dataclass
class TsStock:
    ts_id: str
    stock_code: str
    stock_name: str
    market_type: str
    closing_price: float
    price_change: float
    price_change_rate: float
    opening_price: float
    high_price: float
    low_price: float
    trade_volume: int
    trade_amount: float
    market_cap: float
    listed_shares: int
