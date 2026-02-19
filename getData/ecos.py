import requests
import pandas as pd
import re

class Ecos:
    '''ECOS API client for requesting, parsing, and aggregating statistical data.'''

    def __init__(self, key):
        '''Store API key used for all ECOS requests.'''
        self.key = key

    def requestJson(self, url, print_val=False):
        '''
        Send GET request and return parsed JSON.

        Parameters
        ----------
        url : str
            Full API request URL.
        print_val : bool
            If True, prints request URL.

        Returns
        -------
        dict
            Parsed JSON response.

        Raises
        ------
        ValueError
            If HTTP response status is not 200.
        FileExistsError
            If ECOS API returns INFO-200 error code.
        '''
        if print_val:
            print(self, url)

        response = requests.get(url)
        if response.status_code != 200:
            raise ValueError(f"API request failed with status code: {response.status_code}")

        data = response.json()

        if 'RESULT' in data and data['RESULT']['CODE'] == 'INFO-200':
            raise FileExistsError(f'{url} raise error : {data["RESULT"]["MESSAGE"]}')

        return data

    def getStatDetail(self, keyword, print_val=False, sub_col=None, col_val=str):
        '''
        Search statistics whose names contain keyword and fetch item metadata.

        Parameters
        ----------
        keyword : str
            Substring used to match STAT_NAME.
        print_val : bool
            If True, prints matched tables and items.
        sub_col : str | None
            Optional column name used to filter item rows.
        col_val : str
            Required value when filtering by sub_col.

        Returns
        -------
        dict
            Mapping of statistic or item names to metadata dictionaries.
        '''
        url = f"https://ecos.bok.or.kr/api/StatisticTableList/{self.key}/json/kr/1/1000"
        data = self.requestJson(url)

        candidate_dict = {}
        for row in data['StatisticTableList']['row']:
            if keyword in row['STAT_NAME']:
                candidate_dict[row['STAT_NAME']] = row

        result = {}
        for name, val in candidate_dict.items():
            url_d = f"https://ecos.bok.or.kr/api/StatisticItemList/{self.key}/json/kr/1/1000/{val['STAT_CODE']}"
            try:
                detail = self.requestJson(url_d)
                if print_val:
                    print(f"{name} : ")

                for line in detail.get("StatisticItemList")['row']:
                    if sub_col:
                        if line[sub_col] == col_val:
                            result[f"{name} - {line['ITEM_NAME']}"] = line
                            if print_val:
                                print(f"    {line}")
                    else:
                        result[f"{name} - {line['ITEM_NAME']}"] = line
                        if print_val:
                            print(f"    {line}")
            except:
                pass

            result[name] = val
            if print_val:
                print(f"    {val}")

        return result

    def generateECOSData(self, code=list, period="D", start_date="20200101", end_date="20251101"):
        '''
        Request raw time-series data from ECOS API.

        Parameters
        ----------
        code : list
            [stat_code] or [stat_code, item_code].
        period : str
            Frequency code ("A","Q","M","D","S","SM").
        start_date : str
            Query start date formatted for selected period.
        end_date : str
            Query end date formatted for selected period.

        Returns
        -------
        dict
            Raw JSON response.

        Raises
        ------
        Exception
            If more than two codes are supplied.
        '''
        if len(code) == 1:
            code1, code2 = code[0], None
        elif len(code) == 2:
            code1, code2 = code
        else:
            raise Exception(f"len of code is more than 2 : {len(code)}")

        if code2 is None:
            url = f"https://ecos.bok.or.kr/api/StatisticSearch/{self.key}/json/kr/1/100000/{code1}/{period}/{start_date}/{end_date}"
        else:
            url = f"https://ecos.bok.or.kr/api/StatisticSearch/{self.key}/json/kr/1/100000/{code1}/{period}/{start_date}/{end_date}/{code2}"

        return self.requestJson(url)

    def parseTime(self, val):
        '''
        Convert ECOS time string into pandas Timestamp.

        Supports yearly, monthly, daily, quarterly,
        semiannual, and semimonthly formats.

        Returns
        -------
        pandas.Timestamp | pandas.NaT
        '''
        pattern = re.compile(r"""
            ^
            (?P<y>\d{4})
            (?:
                (?P<m>\d{2})
                (?:
                    (?P<d>\d{2})
                    |
                    S(?P<sm>[12])
                )?
                |
                Q(?P<q>[1-4])
                |
                S(?P<s>[12])
            )?
            $
        """, re.X)

        m = pattern.match(str(val))
        if not m:
            return pd.NaT

        y = int(m["y"])

        if m["q"]:
            return pd.Timestamp(y, (int(m["q"]) - 1) * 3 + 1, 1)

        if m["s"]:
            return pd.Timestamp(y, (int(m["s"]) - 1) * 6 + 1, 1)

        if m["sm"]:
            return pd.Timestamp(y, int(m["m"]), 1 if m["sm"] == "1" else 16)

        if m["d"]:
            return pd.Timestamp(y, int(m["m"]), int(m["d"]))

        if m["m"]:
            return pd.Timestamp(y, int(m["m"]), 1)

        return pd.Timestamp(y, 1, 1)

    def processECOSData(self, data=dict):
        '''
        Transform raw ECOS response into structured DataFrame.

        Parameters
        ----------
        data : dict
            JSON returned from StatisticSearch API.

        Returns
        -------
        DataFrame
            Time-indexed values.
        dict
            Metadata describing statistic.
        '''
        rows = data.get("StatisticSearch", {}).get("row", [])
        if not rows:
            return pd.DataFrame(), {}

        first = rows[0]
        stat_name = re.sub(r'^[\d\.]+\s*', '', first["STAT_NAME"])
        data_detail = first

        df = pd.DataFrame(rows)
        df["TIME"] = df["TIME"].map(self.parseTime)
        df["DATA_VALUE"] = pd.to_numeric(df["DATA_VALUE"], errors="coerce")

        for col in ["ITEM_NAME4","ITEM_NAME3","ITEM_NAME2","ITEM_NAME1"]:
            if col in df.columns and df[col].notna().any():
                name_col = col
                break
        else:
            df = df[["TIME","DATA_VALUE"]].set_index("TIME")
            df.columns = [stat_name]
            return df, data_detail

        df["col"] = df[name_col].astype(str).str.strip()

        df = df.pivot_table(
            index="TIME",
            columns="col",
            values="DATA_VALUE",
            aggfunc="first"
        ).sort_index()

        df.columns = [stat_name if c == "" else f"{stat_name}_{c}" for c in df.columns]

        return df, data_detail

    def getECOSData(self, codes, method="value", start_date="20230101", end_date="20260101", return_detail=False):
        '''
        Download and merge multiple ECOS statistics.

        Parameters
        ----------
        codes : list[tuple]
            Each element = (period_code, code_list).
        method : str
            Reserved parameter (unused).
        start_date : str
            YYYYMMDD.
        end_date : str
            YYYYMMDD.
        return_detail : bool
            If True, also return metadata.

        Returns
        -------
        DataFrame | (DataFrame, dict)
            Combined dataset and optional details.

        Raises
        ------
        ValueError
            If unsupported period code.
        '''
        period_map = {
            "A": lambda d: d[:4],
            "Q": lambda d: d[:4] + "Q1",
            "M": lambda d: d[:6],
            "D": lambda d: d,
            "S": lambda d: d[:4] + "S1",
            "SM": lambda d: d[:6] + "S1"
        }

        dfs = []
        details = {}

        for code in codes:
            per, code_data = code

            if per not in period_map:
                raise ValueError(f"period is not valid: {per}")

            start_var = period_map[per](start_date)
            end_var = period_map[per](end_date)

            try:
                data = self.generateECOSData(code=code_data, period=per, start_date=start_var, end_date=end_var)
                processed_data, detail = self.processECOSData(data)
                dfs.append(processed_data)
                details[code_data[0]] = detail
            except:
                print(f"fail to download {code_data} - {per} - {start_var} - {end_var}")

        df = pd.concat(dfs, axis=1).reset_index().rename(columns={"TIME":"date"})

        return (df, details) if return_detail else df

    def getCode(self, code_dict=dict, include_subcols=True):
        '''
        Convert ECOS metadata dict into request tuple.

        Parameters
        ----------
        code_dict : dict
            Metadata row describing statistic.
        include_subcols : bool
            Whether to include sub-item codes.

        Returns
        -------
        list | None
            [period, [codes]] or None if excluded.
        '''
        if code_dict['CYCLE'] is None:
            url = f"https://ecos.bok.or.kr/api/StatisticTableList/{self.key}/json/kr/1/1000"
            data = self.requestJson(url)

            for row in data['StatisticTableList']['row']:
                if row['CYCLE'] is not None:
                    cycle_val = row['CYCLE']
                    continue

            result = [cycle_val, [code_dict['STAT_CODE']]]
        else:
            cycle_val = code_dict['CYCLE']

        if 'ITEM_CODE' in code_dict:
            result = [cycle_val, [code_dict['STAT_CODE'], code_dict['ITEM_CODE']]]
        else:
            result = [cycle_val, [code_dict['STAT_CODE']]] if include_subcols else None

        return result

    def getCodes(self, codes_dict=dict, include_subcols=True):
        '''
        Convert multiple metadata rows into request list.

        Parameters
        ----------
        codes_dict : dict
            Mapping of names → metadata dict.
        include_subcols : bool
            Include sub-item codes.

        Returns
        -------
        list
            List of request tuples usable in getECOSData().
        '''
        result = []
        for code_dict in codes_dict.values():
            req = self.getCode(code_dict, include_subcols=include_subcols)
            if req is not None:
                result.append(req)
        return result
