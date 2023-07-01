#!/usr/bin/env python3
import requests
import time

def fetch_data():
    res = requests.get("https://api.weather.gov/alerts/active")
    return res.json()

if __name__ == '__main__':
    data = fetch_data()
    print("starting...")
    # now = time.perf_counter()
    for feat in data["features"]:
        geometry = feat["geometry"]
        if geometry is not None:
            res = requests.post("http://localhost:6969/polygon/block_group", json=geometry)
            j = res.json()
            sum = 0
            for thing in j:
                sum += thing["pop"]
            print("geo:", sum)
        else:
            for code in feat["properties"]["geocode"]["SAME"]:
                res = requests.get(f"http://localhost:6969/county/{code[1:]}/pop")
                j = res.json()
                if len(j) > 0:
                    print("county: ", j[0]["pop"])
    # print(time.perf_counter() - now)
    
