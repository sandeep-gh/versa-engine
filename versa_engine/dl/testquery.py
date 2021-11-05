import requests

dlapi_url = "http://pi4bsdm2.monallabs.in/w/api.php"

params = {
    "action": "query",
    "format": "json",
    "list": "prefixsearch",
    "pssearch": "n"
}
S = requests.Session()
result = S.get(url=dlapi_url, params=params)
print("result = ", result)
