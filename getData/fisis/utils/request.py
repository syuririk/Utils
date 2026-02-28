import requests

ERR_DICT = {
    "000": "정상",
    "010": "미등록 인증키",
    "011": "중지된 인증키",
    "012": "삭제된 인증키",
    "013": "샘플 인증키",
    "020": "일일검색 허용횟수 초과",
    "021": "허용된 IP가 아님",
    "022": "허용된 언어가 아님",
    "100": "요청값 누락",
    "101": "잘못된 요청값",
    "900": "정의되지 않은 오류"
}

class FisisAPIError(Exception):
    pass


def getRequest(url, print_url=True):

    if print_url:
        print(url)

    res = requests.get(url)

    if res.status_code != 200:
        raise FisisAPIError(f"HTTP 오류: {res.status_code}")

    data = res.json()

    result = data.get("result", {})
    err_cd = result.get("err_cd")

    if err_cd != "000":
        msg = ERR_DICT.get(err_cd, "알 수 없는 오류")
        raise FisisAPIError(f"[{err_cd}] {msg}")

    return data

