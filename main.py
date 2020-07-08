import argparse
import collections
import datetime
import itertools
import jsonpickle
import locale
import os
import re
import shutil
import stat
import sys
import time
import typing
from collections import namedtuple
from typing import Any, Dict, Generator, Iterable, List, Optional

import recordclass as recordclass
import requests
from bs4 import BeautifulSoup
from flask import Flask
from googleapiclient.discovery import build
from httpcache import CachingHTTPAdapter
from httplib2 import Http
from oauth2client import client, file, tools
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from emailer import Emailer
from fetcher import Fetcher, ListingCache
from listing import LISTING_FIELDS, Listing
from sheets import SheetsRenderer

# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

retry_strategy = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
s = requests.Session()
s.mount("https://", adapter)
s.mount("http://", adapter)

#http_cache = CachingHTTPAdapter(capacity=1000)
#s.mount("http://", http_cache)
#s.mount("https://", http_cache)

locale.setlocale(locale.LC_ALL, "ja_JP.UTF-8")

SCOPES = "https://www.googleapis.com/auth/spreadsheets"
SPREADSHEET_ID = "1KDESi_sl0COPlf3nKGeeNxXfH9j3BBpUq2mlaHZgKgo"
DB_SPREADSHEET_ID = "1mLyhK5IwUDfQlrEvZCYJwVAltG2Kuriy0olrT9mTwxQ"


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


class Scraper(object):
  def __init__(self, host: str, path: str, timestamp=datetime.datetime.now(), spreadsheet_id=SPREADSHEET_ID):
    self.host = host
    self.path = path
    self.renderer = SheetsRenderer(spreadsheet_id)
    self.fetcher: Fetcher = Fetcher(s, self.host)
    self.listing_cache: ListingCache = ListingCache("/tmp/cache-%s" % host, self.fetcher)
    self.emailer = Emailer(self.host, "/tmp/email_log")
    self.timestamp = timestamp

  def Render(self, listing: Listing, fields=None) -> List[str]:
    if not fields:
      fields = LISTING_FIELDS
    n = lambda x: dict(numberValue=x)
    s = lambda x: dict(stringValue=x)
    f = lambda x: dict(formulaValue=x)
    for field in fields:
      if field == "pickle":
        yield s(jsonpickle.encode(listing))
        continue
      if field == "id":
        yield s(listing.id())
        continue
      value = getattr(listing, field)
      if value is None:
        yield s("None")
      elif field == "link":
        yield s("http://%s%s" % (self.host, value))
      elif hasattr(value, "parsed"):
        yield n(value.value) if value.parsed else s(str(value))
      elif field == "address":
        yield f('=HYPERLINK("google.com/maps/place/%s", "%s")' % (value, value))
      elif field == "images":
        for imglink in value[:20]:
          yield f('=IMAGE("http://%s%s")' % (self.host, imglink))
          break
      else:
        yield s(str(value))

  def GetSummariesFromFeaturePage(self, soup):
    ul = soup.find("ul", class_="new")
    assert ul
    items = ul.find_all("li")
    for item in items:
      yield from ParseListingSummary(item)

  def GetSummariesFromSerp(self, soup):
    result_list = soup.find("div", class_="result_list")
    assert result_list
    for result in result_list.find_all("div", class_="base"):
      room_table = result.find("table", class_="room")
      if room_table:
        for row in room_table.find_all("tr", class_="clickableRow"):
          match = re.match(r"location.href='(.*)';", row["onclick"])
          if not match:
            print("ERROR: invalid onclick string: [%s]" % row["onclick"])
            continue
          yield Listing(link=match.group(1))
      else:
        link_a = result.find("a")
        if not link_a:
          print("No link in result: [%s]" % result)
        assert link_a
        link = link_a["href"]
        print("ERROR: No room table in result, falling back to building-level link [%s]" % link)
        yield Listing(link=link)
    pager = soup.find("div", class_="pager")
    next_li = pager.find("li", class_="next")
    next_a = next_li.find("a")
    if not next_a:
      print("This was the last page of results")
      return
    next_url = "http://%s%s" % (self.host, next_a["href"])
    print("Moving on to next result page: %s" % next_url)
    page = s.get(next_url)
    next_soup = BeautifulSoup(page.content, "html.parser")
    yield from self.GetSummariesFromSerp(next_soup)

  def FetchSummaries(self) -> List[Listing]:
    url = "http://" + self.host + self.path
    print("Rescan triggered for url %s" % url)
    page = s.get(url)
    soup = BeautifulSoup(page.content, "html.parser")

    feature_ul = soup.find("ul", class_="new")
    search_results = soup.find("div", class_="result_list")
    if feature_ul:
      return self.GetSummariesFromFeaturePage(soup)
    elif search_results:
      return self.GetSummariesFromSerp(soup)
    raise ValueError("Unrecognized start page at %s" % url)

  def Rescan(self) -> Generator[Listing, None, None]:
    for summary in self.FetchSummaries():
      for listing in self.listing_cache.FetchCached(summary.link):
        yield listing

  def UpdateDb(self, db: Dict[str, Listing], counters):
    for summary in self.FetchSummaries():
      counters["total_active"] += 1
      id = summary.id()
      if id not in db.keys():
        counters["new_rooms"] += 1
        db[id] = self.listing_cache.FetchCached(summary.link)[0]
        db[id].firstseen = self.timestamp
      db[id].active = True
      db[id].lastseen = self.timestamp
      db[id].seen_internal = True

    for id in db.keys():
      db[id].PopulateDerived()
      if not db[id].seen_internal:
        if db[id].active:
          counters["newly_inactive"] += 1
        db[id].active = False
        counters["total_inactive"] += 1
    

  def RenderListings(self, listings: Iterable[Listing]):
    listing_dict = {l.id(): l for l in listings}
    listings = sorted(listing_dict.values(),
                      key=lambda x: (x.msq.value if x.msq.parsed else 0),
                      reverse=True)
    tier1 = []
    tier2 = []

    for listing in listings:
      if listing.IsInteresting():
        listing.tier = "tier1"
        tier1.append(listing)
      else:
        listing.tier = "tier2"
        tier2.append(listing)

    reqs = self.renderer.ClearSheetReqs()
    reqs.append(self.renderer.UpdateCellReq(0, 0, [dict(stringValue=f) for f in ["Notes", "pickle"]+LISTING_FIELDS]))
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

  def ReadSiteMap(self):
    url = "http://%s" % self.host
    page = s.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    sitemap_div = soup.find("div", class_="sitemap")
    if not sitemap_div:
      print("No sitemap div found")
      return
    for a in sitemap_div.find_all("a"):
      yield a["href"]


@app.route('/')
def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("--host", default="tomigaya.jp")
  parser.add_argument("--path", default="/feature/new")
  args, _ = parser.parse_known_args()
  print("cwd is: %s" % os.getcwd())
  # Rescan(args.host, args.path)
  scraper = Scraper(args.host, args.path)
  scraper.RenderListings(scraper.Rescan())
  return("Updated!")

@app.route('/custom/<string:host>/<path:subpath>')
def scrape_custom_path(host, subpath):
  print("haha custom go %s|%s" % (host, subpath))
  scraper = Scraper(host, ("/%s" % subpath))
  title = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + " " + host + subpath
  scraper.renderer.CreateAndUseSheet(title)
  scraper.RenderListings(scraper.Rescan())
  return("Done customing")

@app.route('/crawl/<string:host>')
def crawl_sitemap(host):
  timestamp = datetime.datetime.now()
  scraper = Scraper(host, "", timestamp)
  title = datetime.datetime.now().strftime('%m-%d %H:%M:%S') + " " + host + " crawl"
  listing_gens = []
  for link in scraper.ReadSiteMap():
    print("Crawling [%s]"%link)
    sub_scraper = Scraper(host, link, timestamp)
    listing_gens.append(sub_scraper.Rescan())
  scraper.renderer.CreateAndUseSheet(title)
  scraper.RenderListings(itertools.chain(*listing_gens))
  return "Done crawling"

@app.route('/scrape-db/<string:host>/<path:subpath>')
def scrape_and_update_db(host, subpath):
  timestamp = datetime.datetime.now()
  scraper = Scraper(host, ("/%s" % subpath), timestamp, DB_SPREADSHEET_ID)
  counters = collections.defaultdict(int)
  listing_headers = ["id"] + LISTING_FIELDS + ["pickle"]
  if scraper.renderer.CreateAndUseSheet("%s%s db" % (host, subpath)):
    counters["sheet_created"] += 1
    init_reqs = [scraper.renderer.UpdateCellReq(0, 0, [dict(stringValue=f) for f in ["", "", ""]+listing_headers])]
    scraper.renderer.ExecuteReqs(init_reqs)

  db: Dict[str, Listing] = {l.id(): l for l in scraper.renderer.ReadPickleDb()}
  scraper.UpdateDb(db, counters)
  reqs = []
  row = 1

  id_col, id_col_num = scraper.renderer.FindColumn("id")
  for id in scraper.renderer.ReadRange("%s2:%s" % (id_col, id_col), majorDimension="COLUMNS"):
    if not id in db.keys():
      counters["unknown_ids_in_sheet"] += 1
      print("ERROR: id %s not in db" % id)
      continue
    reqs.append(scraper.renderer.UpdateCellReq(row, id_col_num, scraper.Render(db[id], listing_headers)))
    counters["sheet_rows_updated"] += 1
    db[id].written_internal = True
    row += 1

  for id in db.keys():
    if db[id].written_internal:
      continue
    reqs.append(scraper.renderer.UpdateCellReq(row, id_col_num, scraper.Render(db[id], listing_headers)))
    counters["sheet_rows_added"] += 1
    db[id].written_internal = True
    row += 1
  
  if scraper.renderer.CreateAndUseSheet("%s%s history" % (host, subpath)):
    counters["sheet_created"] += 1
    reqs.append(scraper.renderer.UpdateCellReq(0, 0, [dict(stringValue=f) for f in ["timestamp", "counters"]]))
  row = 2
  for value in scraper.renderer.ReadRange("A2:A", majorDimension="COLUMNS"):
    row += 1
    if not value:
      break
  reqs.append(scraper.renderer.UpdateCellReq(row, 0, [dict(stringValue=str(f)) for f in [timestamp, counters]]))
  scraper.renderer.ExecuteReqs(reqs)
  return "<pre>Done. Counters:\n%s</pre>" % "\n".join(["%30s %6d" % (k, v) for k, v in sorted(counters.items())])
      

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)
