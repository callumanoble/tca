"""
    Core code for volume analysis
"""
import pandas as pd


def ADV(volume: pd.DataFrame, window: int) -> pd.DataFrame:
    """
    Calculate rolling ADV over window

    :param volume: Dataframe containing daily volume data
    :param window: Lookback (days)
    :return: Dataframe containing lagged ADV for each date
    """
    adv = volume.shift().rolling(window).mean()
    return adv


def MDV(volume: pd.DataFrame, window: int) -> pd.DataFrame:
    """
    Calculate rolling MDV (median daily volume) over window

    :param volume: Dataframe containing daily volume data
    :param window: Lookback (days)
    :return: Dataframe containing lagged MDV for each date
    """
    adv = volume.shift().rolling(window).median()
    return adv


FILE = "/home/cnoble/Documents/market_data/volume/snp_500_2022-08-26-20-Y.csv"

if __name__ == "__main__":
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option("display.max_colwidth", None)

    volume = pd.read_csv(FILE)
    adv = ADV(volume, 30)
    mdv = MDV(volume, 30)
    print(adv)
    print(mdv)