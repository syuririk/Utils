import numpy as np
import pandas as pd

from . import processUtils


# ======================================================================
# Basic factor generators
# ======================================================================

def ratio_factor(numer, denom) -> Callable[[pd.DataFrame], pd.Series]:
    def factor(df: pd.DataFrame) -> pd.Series:
        if isinstance(numer, (int, float)):
            return processUtils.safe_div(numer, df[denom])
        return processUtils.safe_div(df[numer], df[denom])
    return factor


def pct_change_factor(code, col, periods, date_col=None):
    def factor(df: pd.DataFrame) -> pd.Series:
        if date_col:
            df = df.sort_values(date_col)
        return df.groupby(code, sort=False)[col].pct_change(periods, fill_method=None)
    return factor


def rolling_stat_factor(code, col, window, stat="mean"):
    def factor(df: pd.DataFrame) -> pd.Series:
        g = df.groupby(code, sort=False)[col]

        if stat == "mean":
            out = g.rolling(window).mean()
        elif stat == "std":
            out = g.rolling(window).std()
        else:
            raise ValueError("stat must be 'mean' or 'std'")

        return out.reset_index(level=0, drop=True)

    return factor


def momentum_factor(code, col, period, subtract_period=None):
    def factor(df: pd.DataFrame) -> pd.Series:
        g = df.groupby(code, sort=False)[col]
        base = g.pct_change(period, fill_method=None)

        if subtract_period is not None:
            return base - g.pct_change(subtract_period, fill_method=None)

        return base.replace([np.inf, -np.inf], np.nan).astype(float)

    return factor


def ma_factor(code, col, window):
    def factor(df: pd.DataFrame) -> pd.Series:
        return df.groupby(code, sort=False)[col].transform(
            lambda x: x.rolling(window).mean()
        )
    return factor


# ======================================================================
# Volatility / Liquidity factors
# ======================================================================

def parkinson_vol_factor(code, high, low, window=21):
    def factor(df: pd.DataFrame) -> pd.Series:
        hl = np.log(df[high] / df[low]) ** 2

        return (
            hl.groupby(df[code])
            .rolling(window)
            .mean()
            .mul(1 / (4 * np.log(2)))
            .pow(0.5)
            .reset_index(level=0, drop=True)
        )
    return factor


def volume_surge_factor(code, volume, short=5, long=60):
    def factor(df: pd.DataFrame) -> pd.Series:
        v = df.groupby(code, sort=False)[volume]
        short_ma = v.rolling(short).mean().reset_index(level=0, drop=True)
        long_ma = v.rolling(long).mean().reset_index(level=0, drop=True)
        return processUtils.safe_div(short_ma, long_ma)
    return factor


def amihud_factor(code, price, amount):
    def factor(df: pd.DataFrame) -> pd.Series:
        ret = df.groupby(code, sort=False)[price].pct_change(fill_method=None).abs()
        return processUtils.safe_div(ret, df[amount])
    return factor


# ======================================================================
# Fundamental factors
# ======================================================================

def log_factor(col):
    def factor(df: pd.DataFrame) -> pd.Series:
        return np.log(df[col])
    return factor


def accrual_factor(net_income, cfo, assets):
    def factor(df: pd.DataFrame) -> pd.Series:
        return processUtils.safe_div(df[net_income] - df[cfo], df[assets])
    return factor


def ev_ebit_factor(marcap, debt, assets, ebit):
    def factor(df: pd.DataFrame) -> pd.Series:
        cash_proxy = df[assets] - df[debt]
        ev = df[marcap] + df[debt] - cash_proxy
        return processUtils.safe_div(ev, df[ebit])
    return factor


# ======================================================================
# Utilities
# ======================================================================

def cs_zscore(df, col, eps=1e-8):
    return df.groupby("Date")[col].transform(
        lambda x: (x - x.mean()) / (x.std() + eps)
    )


def compute_factors(df, factor_dict, zscore=True, columns=None):

    if columns is None:
        columns = []

    for i, (name, func) in enumerate(factor_dict.items(), 1):
        print(name, end="    ")
        if i % 5 == 0:
            print()

        df[name] = func(df).astype("float32")
        columns.append(name)

        if zscore:
            zname = f"{name}_Z"
            df[zname] = cs_zscore(df, name).astype("float32")
            columns.append(zname)

    return df[columns]
