import requests
import pandas as pd
import json

class Fred():
  """
  FRED API wrapper class for retrieving category, series, tag, and observation data.

  This class provides utility methods to interact with the FRED API, including category
  browsing, series lookup, tag filtering, and time series data retrieval.

  Parameters
  ----------
  api_key : str
      Valid FRED API key used for all requests.

  Notes
  -----
  - All responses are returned as parsed JSON or pandas DataFrame objects.
  - HTTP errors are printed but execution continues unless JSON parsing fails.
  - SSL verification is disabled for requests.
  """

  def __init__(self, api_key):
    """Store API key for authenticated requests."""
    self.api_key = api_key

  def request(self, url, params=None, print_url=True):
    """
    Send HTTP GET request and return parsed JSON response.

    Parameters
    ----------
    url : str
        API endpoint URL.

    params : dict, optional
        Query parameters.

    print_url : bool, default=True
        If True prints URL and parameters.

    Returns
    -------
    dict
        Parsed JSON response.

    Raises
    ------
    Exception
        If JSON parsing fails.
    """
    if print_url:
      print(url)
      print(params)

    if params is None:
      params = {}

    try:
      response = requests.get(url, params, verify=False)
      response.raise_for_status()
    except requests.exceptions.HTTPError as err:
      print(f"HTTP error occurred: {err}")

    try:
      data = response.json()
    except:
      raise Exception("Invalid JSON response")

    if type(data) == NoneType:
      data = {}
      
    return data


  def search(self, dicts, col, keyword):
    """
    Filter dictionary list by keyword match.

    Parameters
    ----------
    dicts : list[dict]
        Input records.

    col : str
        Column name.

    keyword : str
        Substring to search.

    Returns
    -------
    list[dict]
        Filtered records.
    """
    result = []
    for single_dict in dicts:
      if keyword in single_dict.get(col):
        result.append(single_dict)
    return result


  def getCategoryDetail(self, category_id):
    """
    Retrieve category metadata including child categories.

    Parameters
    ----------
    category_id : str or int

    Returns
    -------
    dict
        Category information with children list.
    """
    url = f"https://api.stlouisfed.org/fred/category"
    params = {
        "category_id" : category_id,
        "api_key" : self.api_key,
        "file_type" : "json"
    }
    data = self.request(url, params=params)
    result = data.get('categories')[0]
    result['children'] = self.getChildren(category_id)
    return result


  def getChildren(self, category_id, start_date=None, end_date=None):
    """
    Retrieve child categories of a category.

    Parameters
    ----------
    category_id : str or int

    start_date : str, optional
    end_date : str, optional

    Returns
    -------
    list[dict]
    """
    url = "https://api.stlouisfed.org/fred/category/children"
    params = {
        "category_id" : category_id,
        "api_key" : self.api_key,
        "file_type" : "json"
    }

    if not start_date == end_date == None:
      params.update(realtime_start=start_date, realtime_end=end_date)

    data = self.request(url, params=params)
    return data.get('categories')


  def getSeriessDetail(self, category_id=0):
    """
    Retrieve series list within a category sorted by popularity.

    Parameters
    ----------
    category_id : str or int, default=0

    Returns
    -------
    list[dict]
    """
    url = f"https://api.stlouisfed.org/fred/category/series"
    params = {
        "category_id" : category_id,
        "api_key" : self.api_key,
        "file_type" : "json",
        "order_by" : "popularity",
        "sort_order" : "desc"
    }
    data = self.request(url, params=params)
    return data.get('seriess')


  def getDatacode(self, id, start_date, end_date):
    """
    Retrieve metadata for a series within a realtime window.

    Parameters
    ----------
    id : str
        Series ID.

    start_date : str
    end_date : str

    Returns
    -------
    list[dict]
    """
    url = f"https://api.stlouisfed.org/fred/series"
    params = {
        "series_id" : id,
        "api_key" : self.api_key,
        "file_type" : "json",
        "realtime_start" : start_date,
        "realtime_end" : end_date
    }
    data = self.request(url, params=params)
    return data.get('seriess')


  def getTags(self, keyword=None):
    """
    Retrieve tag list or filter by keyword.

    Parameters
    ----------
    keyword : str, optional

    Returns
    -------
    list[dict]
    """
    data = self.request(
        f"https://api.stlouisfed.org/fred/tags?api_key={self.api_key}&file_type=json"
    ).get('tags')

    if keyword is None:
      return data
    else:
      return self.search(data, 'name', keyword)


  def getTagSeries(self, tag):
    """
    Retrieve series associated with a tag.

    Parameters
    ----------
    tag : str

    Returns
    -------
    list[dict]
    """
    url = f"https://api.stlouisfed.org/fred/tags/series?tag_names={tag}&api_key={self.api_key}&file_type=json&order_by=popularity&sort_order=desc"
    data = self.request(url)
    return data.get('seriess')


  def generateFredData(self, series_id):
    """
    Retrieve observation data for a series.

    Parameters
    ----------
    series_id : str

    Returns
    -------
    list[dict]
        Observation records.
    """
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "api_key" : self.api_key,
        "file_type" : "json",
        "series_id" : series_id
    }

    data = self.request(url, params=params)
    return data.get('observations')


  def processFredData(self, df):
    """
    Placeholder for future data preprocessing.

    Parameters
    ----------
    df : pandas.DataFrame

    Returns
    -------
    None
    """
    pass


  def getFredData(self, codes, release_date=False):
    """
    Retrieve observation data and convert to DataFrame.

    Parameters
    ----------
    codes : str
        Series ID.

    release_date : bool, default=False
        Reserved parameter (unused).

    Returns
    -------
    pandas.DataFrame
    """
    result = self.generateFredData(codes)
    result = pd.DataFrame(result)
    return result
