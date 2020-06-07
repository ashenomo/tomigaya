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
from typing import Any, List, Dict, Optional, Iterable, Generator
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


class Scraper(object):
  def __init__(self, host: str, path: str, rescan_secs=900):
    self.host = host
    self.path = path
    self.renderer = SheetsRenderer(SPREADSHEET_ID)
    self.rescan_secs = rescan_secs
    self.fetcher: Fetcher = Fetcher(s, self.host)
    self.listing_cache: ListingCache = ListingCache("/tmp/cache-%s" % host, self.fetcher)
    self.emailer = Emailer(self.host, "/tmp/email_log")
    # store = file.Storage('/tmp/token.json')
    # creds = store.get()
    # if not creds or creds.invalid:
    #   flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
    #   creds = tools.run_flow(flow, store)
    # self.service = build('sheets', 'v4', http=creds.authorize(Http()))

  def Render(self, listing: Listing) -> List[str]:
    n = lambda x: dict(numberValue=x)
    s = lambda x: dict(stringValue=x)
    f = lambda x: dict(formulaValue=x)
    yield s("")
    for field in LISTING_FIELDS:
      value = getattr(listing, field)
      if field == "link":
        yield s("http://%s%s" % (self.host, value))
      elif field in ["rent", "msq"]:
        yield n(value.value) if value.parsed else s(str(value))
      elif field == "address":
        yield f('=HYPERLINK("google.com/maps/place/%s", "%s")' % (value, value))
      elif field == "images":
        for imglink in value[:20]:
          yield f('=IMAGE("http://%s%s")' % (self.host, imglink))
      else:
        yield s(value)

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
      link_a = result.find("a")
      if not link_a:
        print("No link in result: [%s]" % result)
      assert link_a
      link = link_a["href"]
      yield Listing(link=link)
      """
      print("Link is %s" % link)
      title_span = result.find("span", recursive=False, class_="room-title")
      print("title_span: %s" % title_span)
      title_strong = title_span.find("strong")
      print("title_span: %s" % title_strong)
      title = title_strong.text
      print("title: %s" % title)
      assert title
      yield Listing(text=title, link=link_a["href"])
      """
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

  def Rescan(self) -> Generator[Listing, None, None]:
    url = "http://" + self.host + self.path
    print("Rescan triggered for url %s" % url)
    page = s.get(url)
    soup = BeautifulSoup(page.content, "html.parser")
    summaries = None

    feature_ul = soup.find("ul", class_="new")
    search_results = soup.find("div", class_="result_list")
    if feature_ul:
      summaries = self.GetSummariesFromFeaturePage(soup)
    elif search_results:
      summaries = self.GetSummariesFromSerp(soup)
    else:
      print("Unrecognized start page at %s" % url)

    for summary in summaries:
      for listing in self.listing_cache.FetchCached(summary.link):
        yield listing

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
    reqs.append(self.renderer.UpdateCellReq(0, 0, [dict(stringValue=f) for f in ["Notes"]+LISTING_FIELDS]))
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
  scraper = Scraper(host, "")
  title = datetime.datetime.now().strftime('%m-%d %H:%M:%S') + " " + host + " crawl"
  listing_gens = []
  for link in scraper.ReadSiteMap():
    print("Crawling [%s]"%link)
    sub_scraper = Scraper(host, link)
    listing_gens.append(sub_scraper.Rescan())
  scraper.renderer.CreateAndUseSheet(title)
  scraper.RenderListings(itertools.chain(*listing_gens))
  return "Done crawling"

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)