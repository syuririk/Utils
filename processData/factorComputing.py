import numpy as np
import pandas as pd

from . import DataProcessUtils


# ======================================================================
# Basic factor generators
# ======================================================================

def ratioFactor(numer, denom):
    def factor(df):
        if isinstance(numer, (int, float)):
            return safe_div(numer, df[denom])
        return safe_div(df[numer], df[denom])
    return factor

def returnFactor(code, col, period, subtract=None, date_col=None):
    def factor(df):
        if date_col:
            df = df.sort_values(date_col)

        g = df.groupby(code)[col]
        base = g.pct_change(period)

        if subtract:
            base -= g.pct_change(subtract)

        return base.replace([np.inf, -np.inf], np.nan)

    return factor


def rollingStatFactor(code, col, window, stat="mean"):
    def factor(df):
        g = df.groupby(code, sort=False)[col]

        if stat == "mean":
            out = g.rolling(window).mean()
        elif stat == "std":
            out = g.rolling(window).std()
        else:
            raise ValueError("stat must be 'mean' or 'std'")

        return out.reset_index(level=0, drop=True)

    return factor

def logFactor(col):
    def factor(df):
        return np.log(df[col])
    return factor

def maCrossFactor(code, col, short=5, long=60, method="ratio"):
    def factor(df):
        g = df.groupby(code, sort=False)[col]
        short_ma = g.rolling(short).mean().reset_index(level=0, drop=True)
        long_ma = g.rolling(long).mean().reset_index(level=0, drop=True)

        if method == "ratio":
            return safe_div(short_ma, long_ma)
        elif method == "diff":
            return short_ma - long_ma
        elif method == "signal":
            return (short_ma > long_ma).astype("float32")
        else:
            raise ValueError("method must be ratio | diff | signal")
    return factor

def parkinsonVolFactor(code, high, low, window=21):
    def factor(df):
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

def amihudFactor(code, price, amount):
    def factor(df):
        ret = df.groupby(code, sort=False)[price].pct_change(fill_method=None).abs()
        return safe_div(ret, df[amount])
    return factor

def compareFactor(left, right, op="gt"):
    def factor(df):
        l = df[left] if isinstance(left, str) else left
        r = df[right] if isinstance(right, str) else right

        if op == "gt":
            out = l > r
        elif op == "ge":
            out = l >= r
        elif op == "eq":
            out = l == r
        elif op == "ne":
            out = l != r
        else:
            raise ValueError("op must be gt | ge | eq | ne")
        return out.astype(int)
    return factor

def rollingZscoreFactor(code, col, window, eps=1e-8):
    def factor(df):
        g = df.groupby(code, sort=False)[col]

        mean = g.rolling(window).mean().reset_index(level=0, drop=True)
        std = g.rolling(window).std().reset_index(level=0, drop=True)

        return ((df[col] - mean) / (std + eps)).astype("float32")
    return factor

    
# ======================================================================
# Utilities
# ======================================================================

def csZscore(df, col, eps=1e-8, date_col="date"):
    return df.groupby(date_col)[col].transform(
        lambda x: (x - x.mean()) / (x.std() + eps)
    )



def computeFactors(df, factor_dict, zscore=True, date_col="date"):
    for i, (name, func) in enumerate(factor_dict.items(), 1):
        print(name, end="    ")
        if i % 5 == 0:
            print()

        df[name] = func(df).astype("float32")

        if zscore:
            zname = f"{name}_Z"
            df[zname] = csZscore(df, name, date_col=date_col).astype("float32")

    return df
