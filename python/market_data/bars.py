import argparse
from datetime import datetime, timedelta

import ibapi.connection
from ib_insync import *
import pandas as pd

def load_bars(connection: ibapi.connection.Connection,
              symbol_type: str,
              symbol: str,
              exchange: str,
              lookback: str,
              bar_horizon: str,
              bar_type: str,
              end_dt: datetime = "",
              regular_hours: bool = True) -> pd.DataFrame:
    """
    Load specified bars from IB TWS

    :param connection: IB insync connection
    :param symbol_type: Asset class str per https://ib-insync.readthedocs.io/api.html#ib_insync.contract.Contract
    :param symbol: Ticker
    :param exchange: Exchange
    :param lookback: Historical lookback range per https://ib-insync.readthedocs.io/api.html#ib_insync.ib.IB.reqHistoricalData
    :param bar_horizon: Bar range per https://ib-insync.readthedocs.io/api.html#ib_insync.ib.IB.reqHistoricalData
    :param bar_type: Data type per https://ib-insync.readthedocs.io/api.html#ib_insync.ib.IB.reqHistoricalData
    :param end_dt: End of date range
    :param regular_hours: Only include regular trading hours
    :return: Dataframe containing bars
    """
    contract = Contract(secType=symbol_type, symbol=symbol, exchange=exchange)
    connection.qualifyContracts(contract)
    cds = util.df(connection.reqContractDetails(contract))
    print(cds.columns)
    print(cds[["marketName", "validExchanges", "tradingHours"]])

    bars = connection.reqHistoricalData(
        contract,
        endDateTime=end_dt,
        durationStr=lookback,
        barSizeSetting=bar_horizon,
        whatToShow=bar_type,
        useRTH=regular_hours)

    # convert to pandas dataframe:
    return util.df(bars)


def main():
    arg_parser = argparse.ArgumentParser("Bar loader")
    arg_parser.add_argument("-t", "--symbol_type",
                            required=False,
                            default="STK",
                            help="Symbol type (default to stock)")
    arg_parser.add_argument("-s", "--symbol",
                            required=True,
                            help="Symbol")
    # TODO - resolve default logic
    arg_parser.add_argument("-x", "--exchange",
                            default="",
                            required=False,
                            help="Exchange (defaults to primary)")
    arg_parser.add_argument("-e", "--end_dt",
                            type=lambda s: datetime.strptime(s, '%Y-%m-%d') if s else "",
                            required=False,
                            default="",
                            help="End of date range")
    arg_parser.add_argument("-l", "--lookback",
                            type=str,
                            required=True,
                            help="Lookback of range")
    arg_parser.add_argument("-b", "--bar_type",
                            type=str,
                            required=True,
                            help="Type of data in bar")
    arg_parser.add_argument("-z", "--bar_horizon",
                            type=str,
                            required=True,
                            help="Bar size")
    arg_parser.add_argument("-r", "--regular_hours",
                            type=bool,
                            required=False,
                            default=True,
                            help="Regular trading hours only")

    args = arg_parser.parse_args()

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    ib = IB()
    with ib.connect('127.0.0.1', 7497, clientId=1) as connection:
        bars = load_bars(
            connection=connection,
            symbol_type=args.symbol_type,
            symbol=args.symbol,
            exchange=args.exchange,
            lookback=args.lookback,
            bar_horizon=args.bar_horizon,
            bar_type=args.bar_type,
            end_dt=args.end_dt,
            regular_hours=args.regular_hours
        )

        print(bars)


if __name__ == "__main__":
    main()
