import argparse
import collections
import datetime
import locale
import os
import re
import stat
import shutil
import sys
import typing
import time
import itertools

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

from listing import Listing
from listing import LISTING_FIELDS
from fetcher import Fetcher
from fetcher import ListingCache
from emailer import Emailer
from sheets import SheetsRenderer

# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

s = requests.Session()
#http_cache = CachingHTTPAdapter(capacity=1000)
#s.mount("http://", http_cache)
#s.mount("https://", http_cache)

locale.setlocale(locale.LC_ALL, "ja_JP.UTF-8")

SCOPES = "https://www.googleapis.com/auth/spreadsheets"
SPREADSHEET_ID = "1KDESi_sl0COPlf3nKGeeNxXfH9j3BBpUq2mlaHZgKgo"


def ParseListingSummary(li) -> Listing:
  # print(li)
  # print("Finding a")
  # print(li.find("a"))
  text = li.find("span", class_="text_area").text
  link = li.find("a")["href"]
  # "background-image:url(/img/room00504273409720e_01t.jpg)">
  imgstyle = li.find("span", class_="img_area")["style"]
  img = re.split(r"\(|\)", imgstyle)[1]
  return Listing(text=text, link=link, images=[img])


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
  def __init__(self, host: str, path: str, rescan_secs=900):
    self.host = host
    self.path = path
    self.renderer = SheetsRenderer(SPREADSHEET_ID)
    self.rescan_secs = rescan_secs
    self.fetcher: Fetcher = Fetcher(s, self.host)
    self.listing_cache: ListingCache = ListingCache("/tmp/cache", self.fetcher)
    self.emailer = Emailer(self.host, "/tmp/email_log")
    # store = file.Storage('/tmp/token.json')
    # creds = store.get()
    # if not creds or creds.invalid:
    #   flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
    #   creds = tools.run_flow(flow, store)
    # self.service = build('sheets', 'v4', http=creds.authorize(Http()))

  """
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
  """

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
    print("Rescan triggered")
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
      for listing in self.listing_cache.FetchCached(summary.link):
        if IsInteresting(listing):
          tier1.append(listing)
        else:
          tier2.append(listing)

    reqs = self.renderer.ClearSheetReqs()
    reqs.append(self.renderer.UpdateCellReq(0, 0, [dict(stringValue=f) for f in LISTING_FIELDS]))
    reqs.append(self.renderer.UpdateCellReq(0, 0, [dict(stringValue="%s 更新" % datetime.datetime.now()),dict(stringValue="")]))
    row = 0
    for listing in tier1:
      row += 1
      reqs.append(self.renderer.UpdateCellReq(row, 0, self.Render(listing)))
    row += 2
    reqs.append(self.renderer.UpdateCellReq(row, 0, [dict(stringValue="以下ゴミ物件")]))
    for listing in tier2:
      row += 1
      reqs.append(self.renderer.UpdateCellReq(row, 0, self.Render(listing)))
    self.renderer.ExecuteReqs(reqs)
    print("MaybeSend email for %d properties" % len(tier1))
    self.emailer.MaybeSend(tier1)

    print("Running id sanity check")
    unique_ids = set()
    for listing in itertools.chain(tier1, tier2):
      id = listing.id()
      unique_ids.add(id)
    print("Num listings: %d, num unique ids: %d" % (len(tier1)+len(tier2), len(unique_ids)))



@app.route('/')
def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--host", default="tomigaya.jp")
  parser.add_argument("--path", default="/feature/new")
  args, _ = parser.parse_known_args()
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