import argparse
import collections
import datetime
import locale
import os
import re
import sys
import typing

import requests
from bs4 import BeautifulSoup
from collections import namedtuple

from httpcache import CachingHTTPAdapter
import recordclass as recordclass
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
from typing import Any, List, Dict, Optional
from flask import Flask

# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

s = requests.Session()
http_cache = CachingHTTPAdapter()
https_cache = CachingHTTPAdapter()
s.mount("http://", http_cache)
s.mount("https://", https_cache)

locale.setlocale(locale.LC_ALL, "ja_JP.UTF-8")

SCOPES = "https://www.googleapis.com/auth/spreadsheets"
SPREADSHEET_ID = "1KDESi_sl0COPlf3nKGeeNxXfH9j3BBpUq2mlaHZgKgo"

LISTING_FIELDS = ["link", "text", "rent", "ldk", "msq", "address",
                  "name", "roomnumber", "leaseterm", "year", "build", "images"]
Listing = recordclass.recordclass("Listing", LISTING_FIELDS,
                                  defaults=(None,) * len(LISTING_FIELDS))


def ParseListingSummary(li) -> Listing:
  # print(li)
  # print("Finding a")
  # print(li.find("a"))
  text = li.find("span", class_="text_area").text
  link = li.find("a")["href"]
  # "background-image:url(/img/room00504273409720e_01t.jpg)">
  imgstyle = li.find("span", class_="img_area")["style"]
  img = re.split("\(|\)", imgstyle)[1]
  return Listing(text=text, link=link, images=[img])


def FetchListingPage(host, listing: Listing) -> Optional[BeautifulSoup]:
  url = "%s%s" % (host, listing.link)
  print("Fetching %s" % url)
  page = s.get(url)
  soup = BeautifulSoup(page.content, "html.parser")
  rooms_table = soup.find("div", class_="table_area scroll-area")
  if rooms_table:
    print("Url %s is a property page, not yet supported" % url)
    return None
  return soup


def NormalizeValue(value: str) -> str:
  normalized = value.strip()
  normalized = re.sub(r"[\s]+", " ", normalized)
  return normalized


class ParsedNumber(object):
  def __init__(self, text: str, unit: str):
    self.text = text
    self.unit = unit
    self.value = None
    self.parsed = False
    norm = text
    if norm.endswith(unit):
      norm = norm[:len(norm) - len(unit)]
    try:
      self.value = locale.atof(norm)
      self.parsed = True
    except ValueError:
      pass

  def __repr__(self):
    if self.parsed:
      return "[%f,%s]" % (self.value, self.unit)
    return "[? %s]" % self.text


def ParseListingPage(page, link: str) -> Listing:
  soup = BeautifulSoup(page.content, "html.parser")
  table = soup.find("table", summary="建物詳細")
  details = collections.defaultdict(str)
  for row in table.find_all("tr"):
    key, value = row.find("th"), row.find("td")
    if not key or not value:
      continue
    details[key.text] = NormalizeValue(value.find(text=True, recursive=False))
  images = [e["href"] for e in soup.find_all("a", class_="sp-slide-fancy")]
  print(details)
  return Listing(
    link=link,
    roomnumber=details["部屋番号"],
    ldk=details["間取り"],
    name=details["物件名称"],
    msq=ParsedNumber(details["専有面積"], "m²"),
    rent=ParsedNumber(details["賃料"], "円"),
    leaseterm=details["契約期間"],
    address=details["所在地"],
    images=images,
    build=details["構造"],
    year=details["築年月"],
  )


def IsInteresting(listing: Listing):
  if listing.msq.parsed and listing.msq.value < 70:
    return False
  if "事務所" in listing.ldk or "店舗" in listing.ldk:
    return False
  if listing.rent.parsed and listing.rent.value > 400000:
    return False
  year = int(listing.year[:4])
  if year < 1981 or (build == "木造" and year < 2001):
    return False
  return True


class Scraper(object):
  def __init__(self, host: str, path: str):
    self.host = host
    self.path = path
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
      flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
      creds = tools.run_flow(flow, store)
    self.service = build('sheets', 'v4', http=creds.authorize(Http()))

  def UpdateCellReq(self, row, col, contents: List[Dict[str, str]]) -> Dict[str, Any]:
    return {
      'updateCells': {
        'rows': {
          'values': [{'userEnteredValue': x} for x in contents]
        },
        'fields': '*',
        'start': {
          'sheetId': 0,
          'rowIndex': row,
          'columnIndex': col
        },
      }
    }

  def ClearSheetReqs(self) -> List[Dict[str, Any]]:
    return [{
      'updateCells': {
        'rows': { 'values': [{'userEnteredValue': {'stringValue': ''}}] },
        'fields': '*',
        'start': { 'sheetId': 0, 'rowIndex': 0, 'columnIndex': 0 },
      }
    },
    {
      'autoFill': {
        'useAlternateSeries': False,
        'range': { 'sheetId': 0, 'startRowIndex': 0, 'startColumnIndex': 0, 'endRowIndex': 1000, 'endColumnIndex': 25 },
      }
    }]

  def ExecuteReqs(self, reqs):
    response = self.service.spreadsheets().batchUpdate(
      spreadsheetId=SPREADSHEET_ID,
      body={'requests': reqs}).execute()

  def FetchAndParseListings(self, link: str):
    print("http_cache size: %d, https_cache size: %d" % (
      len(http_cache.cache._cache),
      len(https_cache.cache._cache)
    ))
    url = "https://%s%s" % (self.host, link)
    print("Fetching %s" % url)
    page = s.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    rooms_table = soup.find("div", class_="table_area scroll-area")
    if rooms_table:
      unit_links = set()
      for row in rooms_table.find_all("tr"):
        a_elem = row.find_next("td").find("a")
        unit_links.add(a_elem["href"])
      for unit_link in sorted(unit_links):
        unit_url = "https://%s%s" % (self.host, unit_link)
        print("Fetching unit page %s" % url)
        unit_page = s.get(unit_url)
        yield ParseListingPage(unit_page, unit_link)
    else:
      yield ParseListingPage(page, link)

  def Render(self, listing: Listing) -> List[str]:
    n = lambda x: dict(numberValue=x)
    s = lambda x: dict(stringValue=x)
    f = lambda x: dict(formulaValue=x)
    for field in LISTING_FIELDS:
      value = getattr(listing, field)
      if field == "link":
        yield s("https://%s%s" % (self.host, value))
      elif field in ["rent", "msq"]:
        yield n(value.value) if value.parsed else s(str(value))
      elif field == "address":
        yield f('=HYPERLINK("google.com/maps/place/%s", "%s")' % (value, value))
      elif field == "images":
        for imglink in value[:20]:
          yield f('=IMAGE("https://%s%s")' % (self.host, imglink))
      else:
        yield s(value)


  def Rescan(self):
    url = "https://" + self.host + self.path
    print("Hello, url: %s" % url)
    page = s.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    ul = soup.find("ul", class_="new")
    if not ul:
      print("No <ul class='new'> found at url [%s]" % url)
      return

    tier1 = []
    tier2 = []

    items = ul.find_all("li")
    summaries = [ParseListingSummary(item) for item in items]
    for summary in summaries:
      for listing in self.FetchAndParseListings(summary.link):
        if IsInteresting(listing):
          tier1.append(listing)
        else:
          tier2.append(listing)

    reqs = self.ClearSheetReqs()
    reqs.append(self.UpdateCellReq(0, 0, [dict(stringValue=f) for f in LISTING_FIELDS]))
    reqs.append(self.UpdateCellReq(0, 0, [dict(stringValue="%s 更新" % datetime.datetime.now()),dict(stringValue="")]))
    row = 0
    for listing in tier1:
      row += 1
      reqs.append(self.UpdateCellReq(row, 0, self.Render(listing)))
    row += 2
    reqs.append(self.UpdateCellReq(row, 0, [dict(stringValue="以下ゴミ物件")]))
    for listing in tier2:
      row += 1
      reqs.append(self.UpdateCellReq(row, 0, self.Render(listing)))
    self.ExecuteReqs(reqs)


@app.route('/')
def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--host", default="tomigaya.jp")
  parser.add_argument("--path", default="/feature/new")
  args = parser.parse_args()
  print("cwd is: %s" % os.getcwd())
  # Rescan(args.host, args.path)
  scraper = Scraper(args.host, args.path)
  scraper.Rescan()
  return("Updated!")


if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)