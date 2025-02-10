import requests

url = "https://4bc3-150-116-182-25.ngrok-free.app/get_strategy2"
proxies = {
    "http": None,
    "https": None  # 強制不使用系統代理，避免 DNS 解析問題
}

res = requests.get(url, proxies=proxies, verify=False)
print(res.status_code)
data = res.json()["data"]
print(data)