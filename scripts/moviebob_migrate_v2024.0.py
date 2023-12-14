#!/usr/bin/env python3

import requests
import re
from bs4 import BeautifulSoup

headers = {
    "referer": "https://letterboxd.com",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}
url = "https://letterboxd.com/essichgurken/film/the-other-zoey/"

soup = BeautifulSoup(requests.get(url, headers=headers).content, "html.parser")
posterDiv = soup.find("div", attrs={"data-target-link": re.compile(r".*")})

if hasattr(posterDiv, "attrs"):
    subUrl = posterDiv.attrs["data-target-link"]
    fullUrl = "https://letterboxd.com" + subUrl
else:
    raise Exception

soup = BeautifulSoup(requests.get(fullUrl, headers=headers).content, "html.parser")
body = soup.find("body")
tmdbId = body.attrs['data-tmdb-id']

print("Found id: %s" % tmdbId)
