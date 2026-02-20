import pandas as pd
import ast
from datetime import datetime

class Krx:
    """
    Wrapper class for retrieving and processing Korea Exchange (KRX) market data using PyKrx.

    This class provides utilities for:
    - ticker and index lookup
    - ETF and sector information
    - index component history
    - OHLCV retrieval
    - active period tracking for securities

    Notes
    -----
    - Uses PyKrx as the underlying data source.
    - All dates must be provided as YYYYMMDD strings unless otherwise stated.
    """

    def __init__(self):
        """
        Initialize KRX environment data.

        Fetches ticker lists, index lists, sector classifications,
        ETF lists, and determines the most recent business day.
        """
        from pykrx import stock
        self.last_Bday = stock.get_nearest_business_day_in_a_week()

        self.KOSPI_stocks = stock.get_market_ticker_list(market="KOSPI")
        self.KOSDAQ_stocks = stock.get_market_ticker_list(market="KOSDAQ")
        self.KRX_stocks = stock.get_market_ticker_list(market="KONEX")
        self.ALL_stocks = stock.get_market_ticker_list(market="ALL")

        self.KOSPI_idx = stock.get_index_ticker_list(market="KOSPI")
        self.KOSDAQ_idx = stock.get_index_ticker_list(market="KOSDAQ")
        self.KRX_idx = stock.get_index_ticker_list(market="KRX")
        self.theme_idx = stock.get_index_ticker_list(market="테마")

        self.KOSPI_sector = stock.get_market_sector_classifications(date=self.last_Bday, market="KOSPI")
        self.KOSDAQ_sector = stock.get_market_sector_classifications(date=self.last_Bday, market="KOSDAQ")

        self.ETF_list = stock.get_etf_ticker_list()


    def getName(self, tickers=list, print_names=False):
        """
        Retrieve ticker names.

        Parameters
        ----------
        tickers : list[str]
            List of ticker codes.

        print_names : bool, default=False
            If True, prints ticker and name.

        Returns
        -------
        dict
            Mapping {ticker: name}

        Behavior
        --------
        Attempts lookup in order:
        1. ETF ticker name
        2. Index ticker name
        3. Market ticker name
        """
        result = {}
        for ticker in tickers:
            try:
                result[ticker] = stock.get_etf_ticker_name(ticker)
            except:
                try:
                    result[ticker] = stock.get_index_ticker_name(ticker)
                except:
                    result[ticker] = stock.get_market_ticker_name(ticker)

            if print_names:
                print(ticker, result[ticker])

        return result


    def BuildActivePeriod(self, df=pd.DataFrame):
        """
        Construct active periods for tickers from add/remove history.

        Parameters
        ----------
        df : pd.DataFrame
            Must contain columns:
            date, added, removed, codes

        Returns
        -------
        pd.DataFrame
            Columns: code, start, end

        Behavior
        --------
        Tracks ticker inclusion over time and records start/end dates
        for each active period.
        """
        df = df.reset_index()
        df["added"] = df["added"].apply(lambda x: ast.literal_eval(x) if isinstance(x,str) else x)
        df["removed"] = df["removed"].apply(lambda x: ast.literal_eval(x) if isinstance(x,str) else x)
        df["date"] = pd.to_datetime(df["date"].astype(str))

        df = df.sort_values("date").set_index("date").sort_index()

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


    def compressPeriod(self, df=pd.DataFrame):
        """
        Merge multiple active periods per ticker into a single range.

        Parameters
        ----------
        df : pd.DataFrame
            Columns: code, start, end

        Returns
        -------
        pd.DataFrame
            Aggregated period per ticker.

        Behavior
        --------
        Groups by code and takes:
        - minimum start
        - maximum end
        """
        return df.groupby("code", as_index=False).agg(
            start=("start","min"),
            end=("end","max")
        ).sort_values("code")


    def generateIndexDeposit(self, ticker=str, start_date=str, end_date=str):
        """
        Generate index component membership periods.

        Parameters
        ----------
        ticker : str
            Index ticker.

        start_date : str
        end_date : str

        Returns
        -------
        pd.DataFrame
            Columns: code, start, end

        Behavior
        --------
        Tracks daily component changes and builds active membership periods.
        """
        dates = pd.date_range(start_date, end_date, freq="B")
        records = []
        prev_set = None

        for d in dates:
            date_str = d.strftime("%Y%m%d")
            try:
                cur_set = set(stock.get_index_portfolio_deposit_file(ticker=ticker, date=date_str))
            except:
                continue

            if not cur_set:
                continue

            if prev_set is None:
                records.append({
                    "date": date_str,
                    "codes": sorted(cur_set),
                    "added": sorted(cur_set),
                    "removed": []
                })
                prev_set = cur_set
                continue

            if cur_set == prev_set:
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
        ticker_period = self.BuildActivePeriod(df=df)
        return ticker_period


    def generateohlcv(self, df=pd.DataFrame):
        """
        Retrieve OHLCV data for multiple tickers and merge into long format.

        Parameters
        ----------
        df : pd.DataFrame
            Columns: code, start, end

        Returns
        -------
        pd.DataFrame
            Columns:
            date, code, open, high, low, close, volume

        Behavior
        --------
        1. Compresses overlapping ticker periods.
        2. Downloads OHLCV per ticker.
        3. Filters by original periods.
        4. Concatenates all tickers.
        5. Sorts by date and code.
        """
        if df.empty:
            return pd.DataFrame(columns=['date','code','open','high','low','close','volume'])

        df['orig_start'] = df['start']
        df['orig_end'] = df['end']

        df_compressed = self.compressPeriod(df)

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
                "날짜":"date","시가":"open","고가":"high","저가":"low","종가":"close","거래량":"volume"
            })
            ohlcv['code'] = code
            ohlcv = ohlcv[['date','code','open','high','low','close','volume']]
            ohlcv['date'] = pd.to_datetime(ohlcv['date'])

            periods = df[df['code']==code][['orig_start','orig_end']]
            mask = pd.Series(False,index=ohlcv.index)

            for _, p in periods.iterrows():
                orig_start = pd.to_datetime(p['orig_start'])
                orig_end = pd.to_datetime(p['orig_end'])
                mask |= (ohlcv['date']>=orig_start)&(ohlcv['date']<=orig_end)

            ohlcv = ohlcv[mask]
            all_dfs.append(ohlcv)

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.sort_values(['date','code']).reset_index(drop=True)
        return final_df


    def getohlcv(self, tickers=list, start_date=str, end_date=str):
        """
        Retrieve OHLCV data for tickers within a date range.

        Parameters
        ----------
        tickers : list[str]
        start_date : str
        end_date : str

        Returns
        -------
        pd.DataFrame
        """
        tickers_len = len(tickers)

        com_df = pd.DataFrame({
            "code": tickers,
            "start": [start_date]*tickers_len,
            "end": [end_date]*tickers_len
        })

        df = self.generateohlcv(com_df)
        return df


    def getIndexDeposit(self, ticker=str, start_date=str, end_date=str):
        """
        Retrieve OHLCV for index components during their active periods.

        Parameters
        ----------
        ticker : str
        start_date : str
        end_date : str

        Returns
        -------
        pd.DataFrame
        """
        pdf = self.generateIndexDeposit(ticker, start_date, end_date)
        df = self.generateohlcv(pdf)
        return df


    def getDepositTickers(self, ticker=str):
        """
        Retrieve current component tickers for index or ETF.

        Parameters
        ----------
        ticker : str

        Returns
        -------
        list[str]
        """
        deposit = stock.get_index_portfolio_deposit_file(ticker=ticker)
        if not deposit:
            deposit = stock.get_etf_portfolio_deposit_file(ticker=ticker)
        return deposit


    def getETFfromName(self, keyword=str, print_val=False):
        """
        Search ETF tickers whose name contains a keyword.

        Parameters
        ----------
        keyword : str

        print_val : bool, default=False
            If True prints matches.

        Returns
        -------
        list[list]
            [[ticker, name], ...]
        """
        result = []
        for t, n in list(self.getName(self.ETF_list).items()):
            if keyword in n:
                result.append([t,n])
                if print_val:
                    print(t,n)
        return result
