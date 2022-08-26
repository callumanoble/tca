import argparse
import logging
import sys
from datetime import datetime, timedelta

import ibapi.connection
from ib_insync import *
import pandas as pd

from python.market_data import universe
from python.market_data.universe import Universe


log_format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(stream=sys.stdout,
                    filemode="w",
                    format=log_format,
                    level=logging.INFO)

logger = logging.getLogger()

def load_snp_500_volume(
                 lookback: str,
                 bin_size: str,
                 end_dt: datetime = "",
                 regular_hours: bool = True) -> pd.DataFrame:
    """
    Load S&P 500 consolidated volume data for snp_500 from IB TWS

    :param lookback: Historical lookback range per https://ib-insync.readthedocs.io/api.html#ib_insync.ib.IB.reqHistoricalData
    :param bin_size: Bin size per https://ib-insync.readthedocs.io/api.html#ib_insync.ib.IB.reqHistoricalData
    :param end_dt: End of date range
    :param regular_hours: Only include regular trading hours
    :return: Dataframe containing volume
    """

    symbol_data = []
    univ = universe.load(Universe.SNP_500)

    ib = IB()

    for i, symbol in enumerate(univ["Symbol"]):
        # TODO - add tidier, scoped reconnect logic
        if not ib.isConnected():
            ib.connect('127.0.0.1', 7497, clientId=1, readonly=True)

        try:
            logger.info(f"Loading symbol:[{symbol}] n:[{i}]")

            # NOTE - we use SMART (order router) consolidated volumes as it's the best IB provide (filter for US symbols via. NMS trading class)
            contract_spec = Stock(symbol=symbol, exchange="SMART", currency="USD")
            contract_dets = ib.reqContractDetails(contract_spec)

            #NOTE - filter on NYSE / NASDAQ as hack to get rid of international tickers
            contract_dets = [c for c in contract_dets if c.contract.primaryExchange in {"NYSE", "NASDAQ"}]

            if len(contract_dets) != 1:
                raise ValueError(f"Invalid contract count:[{len(contract_dets)}] for symbol:[{symbol}]")# primary:[{primary}]")

            # query historical volume
            contract = contract_dets[0].contract
            size_increment = contract_dets[0].suggestedSizeIncrement

            cb = ib.reqHistoricalData(
            contract,
            endDateTime=end_dt,
            durationStr=lookback,
            barSizeSetting=bin_size,
            whatToShow="TRADES",
            useRTH=regular_hours)

            volume_df = util.df(cb)[["date", "volume"]]

            # convert from lots to shares
            volume_df["volume"] = volume_df["volume"] * size_increment
            volume_df = volume_df.rename(columns={"volume": symbol})
            volume_df = volume_df.set_index("date")
            symbol_data.append(volume_df)

        except Exception as e:
            logger.error(f"Failed to load symbol:[{symbol}] error:[{e}]")

    if ib.isConnected():
        ib.disonnect()

    # join symbols
    universe_df = pd.concat(symbol_data, axis=1)
    return universe_df


def main():
    arg_parser = argparse.ArgumentParser("Volume loader")
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
    arg_parser.add_argument("-o", "--output_file",
                            type=str,
                            required=True,
                            help="Output file")
    arg_parser.add_argument("-r", "--regular_hours",
                            type=bool,
                            required=False,
                            default=True,
                            help="Regular trading hours only")

    args = arg_parser.parse_args()

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option("display.max_colwidth", None)

    volume_df = load_snp_500_volume(
        lookback=args.lookback,
        bin_size=args.bin_size,
        end_dt=args.end_dt,
        regular_hours=args.regular_hours,
    )

    logger.info(f"Writing volume data to file:[{args.output_file}]")
    volume_df.to_csv(args.output_file, index_label="date")


if __name__ == "__main__":
    main()
