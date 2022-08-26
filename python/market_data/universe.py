"""
    Utilities for loading PIT universes
"""
import argparse
from enum import Enum

import pandas as pd


class Universe(Enum):
    """
    Enumerated universes
    """
    SNP_500 = 0


def load(uni: Universe) -> pd.DataFrame:
    """
    Factory for loading specified universe

    :param uni: Universe enum
    :return: Dataframe containing universe data
    """
    if uni == Universe.SNP_500:
        return pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]

    else:
        raise ValueError(f"Unsupported universe:[{uni}]")


def main():
    arg_parser = argparse.ArgumentParser("Universe loader")
    arg_parser.add_argument("-u", "--universe",
                            type=lambda a: Universe[a],
                            choices=Universe,
                            required=True,
                            help="Universe to load")
    args = arg_parser.parse_args()

    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    universe = load(args.universe)
    print(universe)


if __name__ == "__main__":
    main()
