import requests

url = "https://4bc3-150-116-182-25.ngrok-free.app/get_strategy2"
proxies = {
    "http": None,
    "https": None
}

res = requests.get(url, proxies=proxies, verify=False)
print(res.status_code)
data = res.json()["data"]
print(data)