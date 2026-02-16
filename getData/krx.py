from pykrx import stock
import pandas as pd
import ast
from datetime import datetime

class Krx:
    """
    class for KRX with PyKrx.
    """
    
    def __init__(self):
        """
        Initialize the Krx class by fetching tickers, index tickers, and sector information.
        Sets the most recent business day.
        """
        self.KOSPI_stocks = stock.get_market_ticker_list(market="KOSPI")
        self.KOSDAQ_stocks = stock.get_market_ticker_list(market="KOSDAQ")
        self.KRX_stocks = stock.get_market_ticker_list(market="KONEX")
        self.ALL_stocks = stock.get_market_ticker_list(market="ALL")

        self.KOSPI_idx = stock.get_index_ticker_list(market="KOSPI")
        self.KOSDAQ_idx = stock.get_index_ticker_list(market="KOSDAQ")
        self.KRX_idx = stock.get_index_ticker_list(market="KRX")
        self.theme_idx = stock.get_index_ticker_list(market="테마")

        self.last_Bday = stock.get_nearest_business_day_in_a_week()
        self.KOSPI_sector = stock.get_market_sector_classifications(date=self.last_Bday, market="KOSPI")
        self.KOSDAQ_sector = stock.get_market_sector_classifications(date=self.last_Bday, market="KOSDAQ")

    def overview_tickers(self, tickers=list):
        """
        Print the names of the provided tickers.
        
        Parameters
        ----------
        tickers : list of str
            A list of ticker codes.

        Behavior
        --------
        1. Attempts to print the index ticker names.
        2. If not an index, prints the market ticker names.
        """
        try:
            for ticker in tickers:
                print(ticker, stock.get_index_ticker_name(ticker))
        except:
            for ticker in tickers:
                print(ticker, stock.get_market_ticker_name(ticker))

    def build_ticker_periods(self, df=pd.DataFrame):
        """
        Calculate active periods (start to end) for each ticker based on added/removed records.
        
        Parameters
        ----------
        df : pd.DataFrame
            DataFrame containing columns: date, added, removed, codes.

        Returns
        -------
        pd.DataFrame
            Columns: code, start, end representing active period of each ticker.

        Behavior
        --------
        1. Converts 'added' and 'removed' to list if stored as string.
        2. Iterates over each date to track active tickers.
        3. Stores each active period when a ticker is removed.
        4. Extends active tickers to the last date.
        """
        df = df.reset_index() 
        df["added"] = df["added"].apply(lambda x: ast.literal_eval(x) if isinstance(x,str) else x)
        df["removed"] = df["removed"].apply(lambda x: ast.literal_eval(x) if isinstance(x,str) else x)
        df["date"] = pd.to_datetime(df["date"].astype(str))

        df = df.sort_values("date").set_index("date")
        df = df.sort_index()

        active = {}
        results = []

        for date, row in df.iterrows():
            for code in row["added"]:
                active[code] = date
            for code in row["removed"]:
                if code in active:
                    results.append({"code": code, "start": active[code], "end": date})
                    del active[code]

        last_date = df.index[-1]
        for code, start in active.items():
            results.append({"code": code, "start": start, "end": last_date})

        return pd.DataFrame(results).sort_values(["code","start"])
  
    def compress_periods(self, df=pd.DataFrame):
        """
        Compress multiple periods of the same ticker into a single period with minimum start and maximum end.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame containing columns: code, start, end.

        Returns
        -------
        pd.DataFrame
            Compressed DataFrame with columns: code, start, end.

        Behavior
        --------
        Groups by code and aggregates the minimum start and maximum end for each ticker.
        """
        return df.groupby("code", as_index=False).agg(start=("start","min"), end=("end","max")).sort_values("code")

    def get_index_deposit(self, ticker=str, start_date=str, end_date=str):
        """
        Retrieve component changes for a given index ticker and calculate active periods.

        Parameters
        ----------
        ticker : str
            Index ticker code.
        start_date : str
            Start date in "YYYYMMDD".
        end_date : str
            End date in "YYYYMMDD".

        Returns
        -------
        pd.DataFrame
            Columns: code, start, end showing active periods of components.

        Behavior
        --------
        1. Generates a list of business days between start_date and end_date.
        2. Retrieves current index components for each date.
        3. Determines added or removed tickers compared to the previous date.
        4. Stores each change as a record.
        5. Extends the last record to end_date.
        6. Returns a DataFrame with active periods for each component.
        """
        dates = pd.date_range(start_date, end_date, freq="B")
        records = []
        prev_set = None

        for d in dates:
            date_str = d.strftime("%Y%m%d")
            try:
                cur_set = set(stock.get_index_portfolio_deposit_file(ticker=ticker, date=date_str))
            except Exception:
                print(f"      {d}")
                continue

            if not cur_set:
                print(f"      {d}")
                continue

            if prev_set is None:
                records.append({
                    "date": date_str,
                    "codes": sorted(cur_set),
                    "added": sorted(cur_set),
                    "removed": []
                })
                prev_set = cur_set
                print(f"{d}")
                continue

            if cur_set == prev_set:
                print(f"  {d}")
                continue

            added = cur_set - prev_set
            removed = prev_set - cur_set

            records.append({
                "date": date_str,
                "codes": sorted(cur_set),
                "added": sorted(added),
                "removed": sorted(removed)
            })
            prev_set = cur_set
            print(f"{d}")

        last_bday_str = end_date
        if records and records[-1]["date"] != last_bday_str:
            records.append({
                "date": last_bday_str,
                "codes": sorted(prev_set),
                "added": [],
                "removed": []
            })

        if not records:
            return pd.DataFrame(columns=["codes","added","removed"])

        df = pd.DataFrame.from_records(records).set_index("date")
        ticker_period = self.build_ticker_periods(df=df)
        return ticker_period

  

    def get_combined_ohlcv(self, df=pd.DataFrame):
        """
        Combine OHLCV data for all tickers in a long format based on code and active periods.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame with columns: code, start, end.

        Returns
        -------
        pd.DataFrame
            Long-format OHLCV DataFrame with columns:
            date, code, open, high, low, close, volume.

        Behavior
        --------
        1. Compress overlapping periods per ticker.
        2. Retrieve OHLCV for each ticker using stock.get_market_ohlcv_by_date or stock.get_index_ohlcv_by_date.
        3. Normalize column names.
        4. Concatenate all tickers into one long DataFrame.
        5. Sort by date and code.
        """

        if df.empty:
            return pd.DataFrame(columns=['date', 'code', 'open', 'high', 'low', 'close', 'volume'])

        df['orig_start'] = df['start']
        df['orig_end'] = df['end']

        df_compressed = self.compress_periods(df)

        all_dfs = []
        code_len = len(list(df_compressed.iterrows()))
        count = 0

        for _, row in df_compressed.iterrows():
            count += 1
            if count % 10 == 0:
                print(f"{count}/{code_len}")

            code = row['code']
            start = row['start']
            end = row['end']

            ohlcv = stock.get_market_ohlcv_by_date(start, end, code)

            ohlcv = ohlcv.reset_index().rename(columns={
                "시가": "open",
                "고가": "high",
                "저가": "low",
                "종가": "close",
                "거래량": "volume",
            })
            ohlcv['code'] = code
            ohlcv = ohlcv[['날짜', 'code', 'open', 'high', 'low', 'close', 'volume']]
            ohlcv = ohlcv.rename(columns={'날짜':'date'})
            ohlcv['date'] = pd.to_datetime(ohlcv['date'])

            periods = df[df['code'] == code][['orig_start','orig_end']]
            mask = pd.Series(False, index=ohlcv.index)
            for _, p in periods.iterrows():
                orig_start = pd.to_datetime(p['orig_start'])
                orig_end = pd.to_datetime(p['orig_end'])
                mask |= (ohlcv['date'] >= orig_start) & (ohlcv['date'] <= orig_end)
            ohlcv = ohlcv[mask]

            all_dfs.append(ohlcv)

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.sort_values(['date', 'code']).reset_index(drop=True)
        return final_df

    def get_ohlcv_list(self, tickers=list, start_date=str, end_date=str):
        """
        Generate a long-format OHLCV DataFrame for a list of tickers over a specified date range.

        Parameters
        ----------
        tickers : list of str
            List of ticker codes to query.
        start_date : str
            Start date in "YYYYMMDD" format.
        end_date : str
            End date in "YYYYMMDD" format.

        Returns
        -------
        pd.DataFrame
            Long-format OHLCV DataFrame with columns: date, code, open, high, low, close, volume.

        Behavior
        --------
        1. Creates a temporary DataFrame containing each ticker and the start/end dates.
        2. Calls get_combined_ohlcv to fetch and combine OHLCV data for all tickers.
        3. Returns the resulting long-format DataFrame.
        """
        tickers_len = len(tickers)
        com_df = pd.DataFrame({
            "code": tickers,
            "start": [start_date]*tickers_len,
            "end": [end_date]*tickers_len
        })
        df = self.get_combined_ohlcv(com_df)
        return df
    def get_index_deposit_df(self, ticker=str, start_date=str, end_date=str):
      """
      Retrieve component changes for a given index ticker and calculate active periods.

      Parameters
      ----------
      tickers : list of str
          List of ticker codes to query.
      start_date : str
          Start date in "YYYYMMDD" format.
      end_date : str
          End date in "YYYYMMDD" format.

      Returns
      -------
      pd.DataFrame
          Long-format OHLCV DataFrame with columns: date, code, open, high, low, close, volume.

      Behavior
      --------
      1. Creates a temporary DataFrame containing each ticker and the start/end dates.
      2. Calls get_combined_ohlcv to fetch and combine OHLCV data for all tickers.
      3. Returns the resulting long-format DataFrame.
      """
      pdf = self.get_index_deposit(ticker, start_date, end_date)
      df = self.get_combined_ohlcv(pdf)
      return df