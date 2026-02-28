from fisis.utils.request import getRequest
from fisis.utils.api import API


def getStatList(partDiv: str):
    '''

    A : 국내은행
    J : 외은지점
    H : 생명보험
    I : 손해보험
    F : 투자매매중개업자Ⅰ
    W : 투자매매중개업자Ⅱ
    G : 집합투자업자
    X : 투자자문일임업자
    D : 종합금융회사
    C : 신용카드사
    K : 리스사
    T : 할부금융사
    N : 신기술금융사
    E : 상호저축은행
    O : 신용협동조합
    Q : 농업협동조합
    P : 수산업협동조합
    S : 산림조합
    M : 부동산신탁
    L : 금융지주회사
    B : 공통(신탁)
    R : 공통(파생상품)


    '''
    api_key = API.get_api_key()

    url = 'http://fisis.fss.or.kr/openapi/companySearch.json?auth={api_key}&partDiv={partDiv}&lang=kr'
    getRequest()