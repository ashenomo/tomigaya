import locale
import re
import recordclass
import urllib.parse
from typing import Any, Dict, Optional, Tuple


LISTING_FIELDS = [
    "active",
    "grentable",
    "link",
    "text",
    "rent",
    "ldk",
    "msq",
    "address",
    "name",
    "roomnumber",
    "leaseterm",
    "year",
    "build",
    "images",
    "firstseen",
    "lastseen",
    "seen_internal",  # To keep track of internal iteration during db update.
    "written_internal",  # To keep track of listings written to sheet in first pass.
]


class Listing(
    recordclass.recordclass(
        "Listing", LISTING_FIELDS, defaults=(None,) * len(LISTING_FIELDS)
    )
):
    def id(self):
        """Returns a presumed-unique id for the room in the form of (building id)___(room number)."""
        # /id/1234 or /id/1234/56
        if not self.link:
            print("ERROR: No link. %s" % self)
            return None
        parts = Listing.parselink(self.link)
        if parts is None:
            return None
        building, room = parts
        if room is not None:
            room = urllib.parse.unquote_plus(room)
            if self.roomnumber is None:
                self.roomnumber = room
            elif room != self.roomnumber:
                print(
                    "ERROR: '%s' != '%s', Room number in link [%s] doesn't match room number in item [%s]"
                    % (room, self.roomnumber, self.link, self)
                )
        return "___".join([building, self.roomnumber])

    def PopulateDerived(self):
        self.grentable = self.IsGrentable()

    def IsGrentable(self):
        if "事務所" in self.ldk or "店舗" in self.ldk:
            return False
        try:
            year = int(self.year[:4])
            if year < 1981 or ("木造" in self.build and year < 2001):
                return False
        except ValueError:
            pass
        return True

    def IsInteresting(self):
        if not self.IsGrentable():
            return False
        if self.msq.parsed and self.msq.value < 70:
            return False
        if self.rent.parsed and self.rent.value > 400000:
            return False
        return True

    @staticmethod
    def fromdict(dict_: Dict[str, Any]):
        args = [dict_.get(k, None) for k in LISTING_FIELDS]
        return Listing(*args)

    @staticmethod
    def parselink(link: str) -> Optional[Tuple[str, Optional[str]]]:
        parts = link.split("/")
        if len(parts) < 3 or len(parts) > 5:
            print("ERROR: Invalid link [%s]" % (link))
            return None
        if len(parts) == 3:
            return parts[2], None
        return parts[2], parts[3]


def NormalizeValue(value: str) -> str:
    normalized = value.strip()
    normalized = re.sub(r"[\s]+", " ", normalized)
    return normalized


class ParsedNumber(
    recordclass.recordclass(
        "ParsedNumber", ["text", "unit", "value", "parsed"], [None, None, None, False]
    )
):
    @staticmethod
    def Parse(text: str, unit: str):
        value = None
        parsed = False
        norm = text
        if norm.endswith(unit):
            norm = norm[: len(norm) - len(unit)]
        try:
            value = locale.atof(norm)
            parsed = True
        except ValueError:
            pass
        return ParsedNumber(text, unit, value, parsed)

    def __repr__(self):
        if self.parsed:
            return "[%f,%s]" % (self.value, self.unit)
        return "[? %s]" % self.text
