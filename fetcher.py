import os
import collections
import jsonpickle
from listing import Listing, NormalizeValue, ParsedNumber
from typing import Optional, List
from bs4 import BeautifulSoup

class Fetcher(object):
    def __init__(self, session, host):
        self.session = session
        self.host = host

    def _ParseListingPage(self, page, link: str) -> Listing:
        soup = BeautifulSoup(page.content, "html.parser")
        table = soup.find("table", summary="建物詳細")
        if not table:
            return
        details = collections.defaultdict(str)
        for row in table.find_all("tr"):
            key, value = row.find("th"), row.find("td")
            if not key or not value:
                continue
            details[key.text] = NormalizeValue(value.find(text=True, recursive=False))
        images = [e["href"] for e in soup.find_all("a", class_="sp-slide-fancy")]
        #print(details)
        yield Listing(
            link=link,
            roomnumber=details["部屋番号"],
            ldk=details["間取り"],
            name=details["物件名称"],
            msq=ParsedNumber.Parse(details["専有面積"], "m²"),
            rent=ParsedNumber.Parse(details["賃料"], "円"),
            leaseterm=details["契約期間"],
            address=details["所在地"],
            images=images,
            build=details["構造"],
            year=details["築年月"],
        )

    def _ParseSerp(self, link, soup):
        print("ParseSerp %s %s" % (link, soup))
        pass


    def Fetch(self, link):
        url = "http://%s%s" % (self.host, link)
        print("Fetching %s" % url)
        page = self.session.get(url)
        soup = BeautifulSoup(page.content, "html.parser")
        rooms_table = soup.find("div", class_="table_area scroll-area")
        serp_list = soup.find("div", class_="result_list")
        if serp_list:
            yield from self._ParseSerp(link, soup)
        if rooms_table:
            unit_links = set()
            for row in rooms_table.find_all("tr"):
                a_elem = row.find_next("td").find("a")
                unit_links.add(a_elem["href"])
            for unit_link in sorted(unit_links):
                unit_url = "http://%s%s" % (self.host, unit_link)
                print("Fetching unit page %s" % url)
                unit_page = self.session.get(unit_url)
                yield from self._ParseListingPage(unit_page, unit_link)
        else:
            yield from self._ParseListingPage(page, link)


class ListingCache(object):
    def __init__(self, directory, fetcher):
        self.directory = directory
        os.makedirs(self.directory, exist_ok=True)
        self.fetcher: Fetcher = fetcher
        self.ids = set()
        self.building_ids = collections.defaultdict(list)
        self._Refresh()

    def _Refresh(self):
        self.ids = set(os.listdir(self.directory))
        self.building_ids.clear()
        for id in self.ids:
            building = id.split("___")[0]
            self.building_ids[building].append(id)
        #print("Cache refreshed, %d listings, %d buildings" % (len(self.ids), len(self.building_ids)))
        #for b, ids in self.building_ids.items():
        #    print("Building %s --> %s" % (b, ids))

    def _ReadRoomCached(self, id) -> Listing:
        with open(os.path.join(self.directory, id)) as f:
            return jsonpickle.decode(f.read())

    def _ReadBuildingCached(self, building_id) -> List[Listing]:
        return [self._ReadRoomCached(id) for id in self.building_ids[building_id]]

    def _WriteToCache(self, listing: Listing):
        with open(os.path.join(self.directory, listing.id()), "w") as f:
            f.write(jsonpickle.encode(listing))

    def FetchCached(self, link) -> Optional[List[Listing]]:
        parts = Listing.parselink(link)
        if parts is None:
            return None
        building, room = parts
        if room is None and building in self.building_ids.keys():
            return self._ReadBuildingCached(building)
        if room is not None:
            id = "___".join([building, room])
            if id in self.ids():
                return [self._ReadRoomCached(id)]

        # Cache miss
        listings = list(self.fetcher.Fetch(link))
        for listing in listings:
            self._WriteToCache(listing)
        print("Fetched %d items" % len(listings))
        self._Refresh()
        return listings


def main():
    cache = ListingCache("/tmp/cache-tomigaya.jp", None)
    listing = cache._ReadRoomCached("2083463___戸建")
    print(listing)
    print("Year? %s" % listing.year)


if __name__ == "__main__":
    main()

