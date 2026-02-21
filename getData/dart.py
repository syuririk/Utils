import pandas as pd
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
from lxml import etree
import dart_fss as dart_fss
import re

import pandas as pd
class Dart:
  def __init__(self, key, get_corp_code=False):
    self.key = key
    if get_corp_code:
      self.corpCode = self.getCorpCode()
    else:
      self.corpCode = self.recallCorpCode()

  def request(self, url, params=None, print_url=True):
    if print_url:
      print(url)
      print(params)

    if params is None:
      params = {}

    try:
      response = requests.get(url, params)
      response.raise_for_status()
    except requests.exceptions.HTTPError as err:
      print(f"HTTP error occurred: {err}")

    try:
        data = response.json()
    except ValueError as e:
        raise Exception("Invalid JSON response") from e

    if isinstance(data, dict) and data.get("error_code"):
        raise Exception(data.get("error_message", "API error"))

    return data

  def recallCorpCode(self):
    df = pd.read_csv('corpcode.csv')
    return df

  def getCorpCode(self):
    url = "https://opendart.fss.or.kr/api/corpCode.xml"
    params = {"crtfc_key": self.key}
    res = requests.get(url, params=params)

    with zipfile.ZipFile(io.BytesIO(res.content)) as z:
        xml_bytes = z.read("CORPCODE.xml")

    root = ET.fromstring(xml_bytes)

    rows = []
    for item in root.findall("list"):
        row = {child.tag: (child.text or "").strip() for child in item}
        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_csv('corpcode.csv')
    return df

  def searchCode(self, keyword, col='corp_name', search_type='eq'):
    '''
      col : corp_code	corp_name	corp_eng_name	stock_code	modify_date
      type : eq, in
    '''
    if search_type == 'eq':
      result = self.corpCode[self.corpCode[col]==keyword]
    elif search_type == 'in':
      result = self.corpCode[self.corpCode[col].str.contains(keyword, na=False)]

    if len(result) == 1:
      corp_code = df.loc[df[col] == keyword, 'corp_code'].iloc[0]

    return result

  def searchReportList(self, code, start_date, end_date, report_type='a001'):
    reports = dart_fss.filings.search(corp_code=code, bgn_de=start_date, end_de=end_date, pblntf_detail_ty=report_type)
    return reports

  def getReportDetail(self, report):
    pages_data = report.pages
    print("="*100)
    print('pages')
    for page in pages_data:
      title = page.title
      id = page.ele_id
      print(f"{id} : {title}")

  def flattenColumns(self, cols):
    if isinstance(cols, pd.MultiIndex):
      return ['_'.join([str(i) for i in col if i]) for col in cols]
    else:
      return cols

  def getDfs(self, report, keywords=None, ex_cols = ['-', '–', '구분', '구성비', '합계'], return_titles=False):
    dfs = []
    minor_dfs = {}
    count = 0
    for page in report:
      count += 1
      if keywords:
        if not any(k in page.title.replace(" ", "") for k in keywords):
          continue
      print(f"get {page.title}")
      try:
          dfs_tmp = pd.read_html(StringIO(page.html))
          dfs.extend(dfs_tmp)
      except:
          pass

    dfs_titles = []
    count = 0
    
    for df in dfs:
      if not isinstance(df, pd.DataFrame):
        print(f"{df.title} is not DataFrame")
        continue

      df.columns = self.flattenColumns(df.columns)

      s = df.stack()
      mask = pd.to_numeric(s, errors='coerce').isna()
      cols = s[mask].unique().tolist()
      cols = [x for x in cols if x not in ex_cols and not re.search(r'\d', str(x))]

      new_string = '/'.join(map(str, cols))
      dfs_titles.append(new_string)
      count +=1

    dict_tmp = []
    for df_num in range(len(dfs_titles)):
      dict_tmp.append(dfs[df_num])

    if return_titles:
      result = dict_tmp, dfs_titles
    else:
      result = dict_tmp
    return result