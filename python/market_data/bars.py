from datetime import datetime, timedelta

from ib_insync import *
import pandas as pd


def load_stock_bars(symbol: str,
                    exchange: str,
                    end_dt: datetime,
                    lookback: timedelta,
                    bar_size: timedelta,
                    type: str,
                    regular_hours: bool) -> pd.DataFrame:
    """
    Load specified bars from IB TWS

    :param symbol: Ticker
    :param exchange: Exchange
    :param end_dt: End of date range
    :param lookback: Historical lookback
    :param bar_size: Bar range
    :param type: Data type per https://ib-insync.readthedocs.io/api.html#ib_insync.ib.IB.reqHistoricalData
    :param regular_hours: Only include regular trading hours
    :return: Dataframe containing bars
    """

ib = IB()
ib.connect('127.0.0.1', 7497, clientId=1)

contract = Stock(symbol="IOZ", exchange="ASX")
print(contract)

ib.qualifyContracts(contract)
print(contract)

#cds = util.df(ib.reqContractDetails(contract))

#print(cds.columns)
#print(cds[["marketName", "validExchanges"]])

bars = ib.reqHistoricalData(
    contract, endDateTime='', durationStr='30 D',
    barSizeSetting='1 hour', whatToShow='MIDPOINT', useRTH=True)

# convert to pandas dataframe:
df = util.df(bars)
print(df)
