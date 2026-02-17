import numpy as np
import pandas as pd


def describe_df(df: pd.DataFrame) -> pd.DataFrame:
  """
  Generate a summary description of a DataFrame.

  Parameters
  ----------
  df : pandas.DataFrame
    Input DataFrame.

  Returns
  -------
  pandas.DataFrame
    Summary table containing dtype, non-null count, null count,
    and number of unique values for each column.
  """
  summary_df = pd.DataFrame({
    "dtype": df.dtypes,
    "non_null": df.count(),
    "null_cnt": df.isna().sum(),
    "n_unique": df.nunique(),
  })

    return summary_df


def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
  """
  Safely divide two numeric Series, avoiding division-by-zero errors.

  Parameters
  ----------
  numer : pandas.Series
    Numerator values.
  denom : pandas.Series
    Denominator values.

  Returns
  -------
  pandas.Series
    Result of division with infinities and invalid results replaced by NaN.
  """
  result = numer / denom.replace(0, np.nan)
  return result.replace([np.inf, -np.inf], np.nan)