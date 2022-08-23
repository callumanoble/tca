from dataclasses import dataclass
from enum import IntEnum
from math import ceil
from typing import List, Optional


class Side(IntEnum):
    """
    Side enum, value represents sign convention
    """
    BUY = 1
    SELL = -1


@dataclass
class ImplementationShortfall:
    """
    Class for storing market activity implementation shortfall
    """
    trading_cost: float
    opportunity_cost: float
    fees: float


def implementation_shortfall(
        px_arrival: float,
        px_final: float,
        px_exec_avg: float,
        qty_filled: int,
        qty_order: int,
        fees: float) -> ImplementationShortfall:
    """
    Calculate market activity implementation shortfall for order execution (vs. arrival) price

    TODO - investigate argument validation (pydantic, FastAPI like?)

    :param px_arrival: Arrival mid-price ($)
    :param px_final: Final mid-price ($)
    :param px_exec_avg: Average executed price ($)
    :param qty_filled: Filled quantity (shares)
    :param qty_order: Order quantity (shares)
    :param fees: Fees ($)
    :return: Market Activity implementation shortfall
    """

    if qty_filled > qty_order or qty_filled < 0:
        raise ValueError(f"Invalid filled quantity:[{qty_filled}]")

    qty_unfilled = qty_order - qty_filled

    return ImplementationShortfall(
        trading_cost=qty_filled * (px_exec_avg - px_arrival),
        opportunity_cost=qty_unfilled * (px_final - px_arrival),
        fees=fees
    )


def trading_cost(
    px_benchmark: float,
    px_exec_avg: float,
    side: Side
) -> float:
    """
    Calculate trading cost vs. benchmark

    :param px_benchmark: Benchmark price ($)
    :param px_exec_avg: Average executed price ($)
    :param side: Trade side
    :return: Trading Cost (bps)
    """
    return side * ((px_exec_avg - px_benchmark) / px_benchmark) * 10e4


def trading_pnl(
    px_benchmark: float,
    px_exec_avg: float,
    side: Side
) -> float:
    """
    Calculating trading PnL vs. benchmark

    :param px_benchmark: Benchmark price ($)
    :param px_exec_avg: Average executed price ($)
    :param side: Trade side
    :return: Trading PNL (bps)
    """
    return -1 * trading_cost(px_benchmark, px_exec_avg, side)


def vwap_cr() -> float:
    """
    Co-routine for calculating a rolling vwap

    :return: None
    """

    wgt_px = 0.0
    total_qty = 0
    vwap = 0.0

    while True:
        px, qty = yield vwap
        wgt_px += px * qty
        total_qty += qty
        vwap = wgt_px / total_qty


def vwap(px: List[float], qty: List[int]) -> Optional[float]:
    """
    Calculate VWAP on a series of trades

    TODO - revisit with better co-routine knowledge

    :param px: Trade price ($)
    :param qty: Trade quantity (shares)
    :return: VWAP ($)
    """

    vcr = vwap_cr()
    next(vcr())
    vwp = None

    for p, q in zip(px, qty):
        vwp = vcr.send(p, q)

    return vwp


def pwp(qty_order: int, pov: float, px_trade: List[float], qty_trade: List[int]) -> Optional[float]:
    """
    Calculate partition weighted price benchmark on a series of trades

    NOTE - If trade series is insufficient to support order size @ POV exception will be thrown

    TODO - investigate argument validation (pydantic, FastAPI like?) - 0 < pov <= 1 etc.
    TODO - revisit with better co-routine knowledge

    :param qty_order: Percentage of volume (pct decimal)
    :param pov: Percentage of volume (pct decimal)
    :param px_trade: Trade price ($)
    :param qty_trade: Trade quantity (shares)
    :return: PWP ($)
    """

    pwp_qty = ceil(qty_order / pov)

    vcr = vwap_cr()
    next(vcr())
    vwp = None

    for p, q in zip(px_trade, qty_trade):
        if pwp_qty <= 0:
            break

        vwp_q = min(pwp_qty, q)
        vwp = vcr.send(p, vwp_q)
        pwp_qty -= vwp_q

    return vwp


def rpm(px_exec_avg: int, side: Side, px_trade: List[float], qty_trade: List[int]) -> Optional[float]:
    """
    Calculate relative performance measure of an order against it's contemporary trades

    :param px_exec_avg: Average executed price ($)
    :param side: Trade side
    :param px_trade: Contemporaneous trade prices ($)
    :param qty_trade: Contemporaneous trade quantities (shares)
    :return: Relative performance measure (0-1)
    """

    qty_outperform = 0
    qty_total = 0

    for p, q in zip(px_trade, qty_trade):
        outperform = px_exec_avg < p if side == Side.BUY else px_exec_avg > p
        if outperform:
            qty_outperform += q

        qty_total += q

    qty_underperform = qty_total - qty_outperform
    return 0.5 * (1 + qty_outperform / qty_total - qty_underperform / qty_total)

def unrealized_performance():
    """
    TODO - how does this compare to opportunity cost?
    :return:
    """
    ...


# Benchmarks
#
# arrival price
# VWAP
# PWP
# Post-trade price

# Metrics
#
# Part-rate
# Parent fill rates
# Child fill rates

# Algo comparisons
#
# Cost model adjusted horse races
# KS test
# Others?
