import argparse
from datetime import datetime, timedelta

import ibapi.connection
from ib_insync import *
import pandas as pd

def load_us_stock_volume(connection: ibapi.connection.Connection,
                         symbol: str,
                         primary: str,
                         lookback: str,
                         bin_size: str,
                         end_dt: datetime = "",
                         regular_hours: bool = True) -> pd.DataFrame:
    """
    Load specified volume from IB TWS

    :param connection: IB insync connection
    :param symbol: Ticker
    :param primary: Primary exchange
    :param lookback: Historical lookback range per https://ib-insync.readthedocs.io/api.html#ib_insync.ib.IB.reqHistoricalData
    :param bin_size: Bin size per https://ib-insync.readthedocs.io/api.html#ib_insync.ib.IB.reqHistoricalData
    :param end_dt: End of date range
    :param regular_hours: Only include regular trading hours
    :return: Dataframe containing volume
    """

    # pull in contracts matching USD tickers then filter on primary
    contract_spec = Stock(symbol=symbol, currency="USD")
    contracts = [c.contract for c in connection.reqContractDetails(contract_spec) if c.contract.primaryExchange == primary]

    if len(contracts) == 0:
        raise ValueError(f"Failed to find contracts for symbol:[{symbol}] primary:[{primary}]")

    # query historical volume for all contracts
    contract_volume = []

    # load volume for each contract
    for contract in contracts:
        cb = connection.reqHistoricalData(
            contract,
            endDateTime=end_dt,
            durationStr=lookback,
            barSizeSetting=bin_size,
            whatToShow="TRADES",
            useRTH=regular_hours)

        # drop unsupported exchanges
        if not cb:
            continue

        cb_df = util.df(cb)[["date", "volume"]]
        cb_df["exchange"] = contract.exchange
        contract_volume.append(cb_df)

    # join contract data
    volume = pd.concat(contract_volume)

    # calculate consolidated volumes and join
    consolidated = volume.groupby("date").sum()
    consolidated["exchange"] = "CONSOLIDATED"
    consolidated = consolidated.reset_index()
    volume = pd.concat([volume, consolidated])
    volume = volume.sort_values(["date", "exchange"]).reset_index()

    # TODO - automatically load lot size...
    # convert from lots to shares
    volume["volume"] = volume["volume"] * 100
    return volume


def main():
    arg_parser = argparse.ArgumentParser("Volume loader")
    arg_parser.add_argument("-s", "--symbol",
                            required=True,
                            help="Symbol")
    arg_parser.add_argument("-p", "--primary",
                            required=True,
                            help="Primary exchange")
    arg_parser.add_argument("-e", "--end_dt",
                            type=lambda s: datetime.strptime(s, '%Y-%m-%d') if s else "",
                            required=False,
                            default="",
                            help="End of date range")
    arg_parser.add_argument("-l", "--lookback",
                            type=str,
                            required=True,
                            help="Lookback of range")
    arg_parser.add_argument("-b", "--bin_size",
                            type=str,
                            required=True,
                            help="Bin size")
    arg_parser.add_argument("-r", "--regular_hours",
                            type=bool,
                            required=False,
                            default=True,
                            help="Regular trading hours only")

    args = arg_parser.parse_args()

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option("display.max_colwidth", None)

    ib = IB()
    with ib.connect('127.0.0.1', 7497, clientId=1) as connection:
        volume = load_us_stock_volume(
            connection=connection,
            symbol=args.symbol,
            primary=args.primary,
            lookback=args.lookback,
            bin_size=args.bin_size,
            end_dt=args.end_dt,
            regular_hours=args.regular_hours
        )

        print(volume)


if __name__ == "__main__":
    main()
