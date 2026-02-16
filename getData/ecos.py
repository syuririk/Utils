import requests
import pandas as pd
import re

class EcosAPI:
    def __init__(self, key):
        self.key = key

    def get_req(self, url):
        """
            Send an HTTP GET request to the specified URL and return the JSON response.

            Parameters
            ----------
            url : str
                The full request URL to be sent to the API endpoint.

            Returns
            -------
            data : dict
                Parsed JSON response from the server when the request is successful (HTTP status code 200).

            Behavior
            --------
            1. Prints the requested URL to stdout.
            2. Sends a GET request using the requests library.
            3. If the response status code is 200:
                - Parses the response body as JSON and returns it.
            4. If the request fails:
                - Prints an error message containing the HTTP status code.
                - Returns an undefined variable (will raise an error if accessed).
            """
        print(self, url)
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
        else:
            raise ValueError(f"API request failed with status code: {response.status_code}")
        return data

    def search_stat_code(self, keyword):
        """
        Search ECOS statistical tables whose names contain a given keyword,
        then retrieve and print detailed item information for each match.

        Parameters
        ----------
        keyword : str
            Keyword used to filter statistic table names. Matching is performed
            using substring containment against the STAT_NAME field.

        Returns
        -------
        None
            This function prints results directly and does not return a value.

        Behavior
        --------
        1. Requests the full list of statistical tables from the ECOS API.
        2. Iterates through all tables and selects those whose STAT_NAME contains
            the provided keyword.
        3. Stores matching tables in a dictionary:
            {stat_name: table_metadata}
        4. For each matched table:
            a. Requests its item list using the table's STAT_CODE.
            b. Prints the table name.
            c. Prints each item row from the detail response.
        5. If item retrieval fails or the structure is unexpected:
            - Prints the original table metadata instead.
        """
        url = f"https://ecos.bok.or.kr/api/StatisticTableList/{self.key}/json/kr/1/1000"
        all_data = self.get_req(url)

        candidate_dict = {}
        for val in all_data['StatisticTableList']['row']:
            if keyword in val['STAT_NAME']:
                candidate_dict[val['STAT_NAME']] = val
        for key, val in candidate_dict.items():
            url_d = f"https://ecos.bok.or.kr/api/StatisticItemList/{self.key}/json/kr/1/1000/{val['STAT_CODE']}"
            detail = self.get_req(url_d)
            print(key, " : ")
            try:
                for line in detail["StatisticItemList"]['row']:
                    print(f"    {line}")
            except:
                print(val)

    def get_data_ecos_stat_search(self, code=list, period="D", start_date="20200101", end_date="20251101"):
        """
        Retrieve statistical time series data from the ECOS API for a given code
        and date range.

        Parameters
        ----------
        code : list
            A list containing one or two code identifiers:
                [code1]              → basic query
                [code1, code2]       → query with sub-item classification
            If more than two elements are provided, an exception is raised.

        period : str, optional, default="D"
            Data frequency code. Common values:
                "A"  : annual
                "Q"  : quarterly
                "M"  : monthly
                "D"  : daily
                "S"  : semiannual
                "SM" : semimonthly

        start_date : str, optional, default="20200101"
            Start date of the query period. Format must match the selected
            period type.

        end_date : str, optional, default="20251101"
            End date of the query period. Format must match the selected
            period type.

        Returns
        -------
        data : dict
            JSON response from the ECOS API containing statistical time series data.

        Raises
        ------
        Exception
            If more than two codes are provided in the `code` list.

        FileExistsError
            If the API returns an INFO-200 result code indicating that the
            request failed or returned no valid data.

        Behavior
        --------
        1. Determines whether the query uses:
            - a single statistic code, or
            - a statistic code with a sub-item code.
        2. Builds the appropriate ECOS API request URL.
        3. Sends the request using `get_req()`.
        4. Checks the API response:
            - If a RESULT field exists and its CODE is "INFO-200",
                raises an error with the API message.
        5. Returns the raw JSON response.

        Notes
        -----
        - Uses a maximum row limit of 100000 per request.
        - Requires a valid API key stored in global variable `KEY`.
        - The function does not validate date format correctness beyond passing
            it directly to the API.
        - A debug value (12314) is printed before raising the error condition.
        """
        if len(code) == 1:
            code1 = code[0]
            code2= None
        elif len(code) == 2:
            code1 = code[0]
            code2 = code[1]
        else:
            raise Exception(f"len of code is more than 2 : {len(code)}")

        if code2 == None:
            url = f"https://ecos.bok.or.kr/api/StatisticSearch/{self.key}/json/kr/1/100000/{code1}/{period}/{start_date}/{end_date}"
        else:
            url = f"https://ecos.bok.or.kr/api/StatisticSearch/{self.key}/json/kr/1/100000/{code1}/{period}/{start_date}/{end_date}/{code2}"

        data = self.get_req(url)

        if 'RESULT' in data:
            if data['RESULT']['CODE'] == 'INFO-200':
                raise FileExistsError(f'{code1} - {code2} - {period} raise error : {data["RESULT"]["MESSAGE"]}')
        return data

    def parse_time(self, val):
        """
        Parse a time string from ECOS statistical data into a pandas Timestamp.

        Parameters
        ----------
        val : str or int
            Time value in one of several ECOS-supported formats. Supported patterns:

            YYYY            → yearly
            YYYYMM          → monthly
            YYYYMMDD        → daily
            YYYYQn          → quarterly (n = 1–4)
            YYYYSn          → semiannual (n = 1–2)
            YYYYMMSn      → semimonthly (n = 1–2; internally formatted as YYYYMM S1/S2)

        Returns
        -------
        pandas.Timestamp or pandas.NaT
            Parsed timestamp representing the beginning of the corresponding period.
            Returns pandas.NaT if the input does not match any recognized format.

        Parsing Rules
        -------------
        - Year only → January 1 of that year.
        - Month → first day of month.
        - Day → exact date.
        - Quarter → first day of first month in quarter:
            Q1 → Jan 1
            Q2 → Apr 1
            Q3 → Jul 1
            Q4 → Oct 1
        - Semiannual → first day of first month in half:
            S1 → Jan 1
            S2 → Jul 1
        - Semimonthly:
            S1 → 1st day of month
            S2 → 16th day of month

        Notes
        -----
        - Input is converted to string before parsing.
        - Regex matching is strict and must match the entire string.
        - The function is designed specifically for ECOS time format conventions.
        """

        time_pattern = re.compile(r"""
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
        """, 
        re.X)
        m = time_pattern.match(str(val))
        if not m:
            return pd.NaT

        y = int(m["y"])

        if m["q"]:
            month = (int(m["q"]) - 1) * 3 + 1
            return pd.Timestamp(y, month, 1)

        if m["s"]:
            month = (int(m["s"]) - 1) * 6 + 1
            return pd.Timestamp(y, month, 1)

        if m["sm"]:
            day = 1 if m["sm"] == "1" else 16
            return pd.Timestamp(y, int(m["m"]), day)

        if m["d"]:
            return pd.Timestamp(y, int(m["m"]), int(m["d"]))

        if m["m"]:
            return pd.Timestamp(y, int(m["m"]), 1)

        return pd.Timestamp(y, 1, 1)

    def process_data_stat_search(self, data=dict):
        """
        Process raw ECOS statistical API response data into a structured DataFrame
        and extract metadata describing the statistic.

        Parameters
        ----------
        data : dict
            Raw JSON-like response returned from the ECOS statistic search API.
            Expected structure:
            {
                "StatisticSearch": {
                    "row": [ {...}, {...}, ... ]
                }
            }

        Returns
        -------
        df : pandas.DataFrame
            A DataFrame indexed by parsed time values.
            Columns represent statistic values.
            If item category columns exist, each category becomes a separate column.
            Otherwise, a single column is returned using the statistic name.

        data_detail : dict
            Dictionary containing metadata describing the statistic:
                - stat_code : str - Statistic code identifier.
                - stat_name : str - Cleaned statistic name (numeric prefixes removed).
                - unit_name : str - Measurement unit.

        Behavior
        --------
        1. Extracts row data from the API response.
        2. Returns empty results if no rows are present.
        3. Builds metadata using the first row.
        4. Converts:
            - TIME → parsed datetime using `parse_time`
            - DATA_VALUE → numeric (invalid values become NaN)
        5. Detects the deepest available item name column among:
            ITEM_NAME4 → ITEM_NAME3 → ITEM_NAME2 → ITEM_NAME1
        6. If no item name column exists:
            - Returns a single-column DataFrame indexed by TIME.
        7. If item name column exists:
            - Pivots the table so each item becomes a column.
            - Column names are formatted as:
                "{stat_name}_{item}"
            (or just stat_name if item label is empty)

        Notes
        -----
        - The statistic name is cleaned using regex to remove leading numbering
        such as "1. ", "01 ", etc.
        - Pivot aggregation uses `first` to resolve duplicate values.
        - Output is sorted chronologically by TIME index.
        """
        rows = data.get("StatisticSearch", {}).get("row", [])
        if not rows:
            return pd.DataFrame(), {}

        first = rows[0]

        stat_code = first.get("STAT_CODE")
        stat_name = first.get("STAT_NAME")

        stat_name = first.get("STAT_NAME")
        stat_name = re.sub(r'^[\d\.]+\s*', '', stat_name)
        data_detail = {
            "stat_code": stat_code,
            "stat_name": stat_name,
            "unit_name": first.get("UNIT_NAME"),
        }

        df = pd.DataFrame(rows)

        df["TIME"] = df["TIME"].map(self.parse_time)
        df["DATA_VALUE"] = pd.to_numeric(df["DATA_VALUE"], errors="coerce")

        for col in ["ITEM_NAME4","ITEM_NAME3","ITEM_NAME2","ITEM_NAME1"]:
            if col in df.columns and df[col].notna().any():
                name_col = col
                break
        else:
            name_col = None

        if name_col is None:
            df = df[["TIME","DATA_VALUE"]].set_index("TIME")
            df.columns = [stat_name]
            return df, data_detail

        df["col"] = df[name_col].astype(str).str.strip()

        df = df.pivot_table(index="TIME", columns="col", values="DATA_VALUE", aggfunc="first").sort_index()

        df.columns = [stat_name if c == "" else f"{stat_name}_{c}" for c in df.columns]

        return df, data_detail

    def get_data_from_ecos(self, codes, method="value", start_date="20230101", end_date="20260101"):

        """
        Retrieve and aggregate statistical data from the ECOS API for multiple codes.

        Parameters
        ----------
        codes : list of tuple
            A list where each element is structured as (period_code, code_data).
            period_code determines the data frequency and must be one of:
            "A" (annual), "Q" (quarterly), "M" (monthly), "D" (daily), "S" (semiannual), "SM" (semimonthly).
            code_data is passed directly to the ECOS query function.

        method : str, optional, default="value"
            Reserved parameter for future functionality. Currently not used.

        start_date : str, optional, default="20230101", format="YYYYMMDD"

        end_date : str, optional, default="20260101", format="YYYYMMDD"

        Returns
        -------
        (df, details)
        df : pandas.DataFrame
            A DataFrame containing all retrieved statistics merged column-wise.
            The index is reset before returning.

        details : dict
            Dictionary mapping each primary code identifier to its corresponding
            metadata or detail information returned from processing.

        Raises
        ------
        ValueError
            If a provided period_code is not supported.

        Behavior
        -----
        1. Date strings are converted automatically to match the format required by each period type.
        2. Data retrieval is performed using `get_data_ecos_stat_search`.
        3. Retrieved data is processed using `process_data_stat_search`.
        4. All resulting DataFrames are concatenated along columns.
        """
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
            per = code[0]
            code_data = code[1]

            if per not in period_map:
                raise ValueError(f"period is not valid: {per}")

            start_var = period_map[per](start_date)
            end_var   = period_map[per](end_date)

            data = self.get_data_ecos_stat_search(code=code_data, period=per, start_date=start_var, end_date=end_var)

            print("="*100)
            print("data :", data)
            processed_data, detail = self.process_data_stat_search(data)

            dfs.append(processed_data)
            details[code[1][0]] = detail

        df = pd.concat(dfs, axis=1)
        df = df.reset_index()

        return df, details