# save as whoami_ip.py
import os, requests
print("PROXY ENV:", {k:v for k,v in os.environ.items() if "PROXY" in k.upper()})
print("requests sees IP:", requests.get("https://api.ipify.org", timeout=10).text)
