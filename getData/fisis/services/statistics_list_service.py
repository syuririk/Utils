from fisis.utils.request import getRequest
from fisis.utils.api import API


def getStatList(partDiv: str):

    api_key = API.get_api_key()

    url = 'http://fisis.fss.or.kr/openapi/companySearch.json?auth={api_key}&partDiv={partDiv}&lang=kr'
    getRequest()