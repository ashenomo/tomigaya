import locale
import re
import recordclass
from typing import Any, Dict, Optional, Tuple


LISTING_FIELDS = ["link", "text", "rent", "ldk", "msq", "address",
                  "name", "roomnumber", "leaseterm", "year", "build", "images"]

class Listing(recordclass.recordclass("Listing", LISTING_FIELDS, defaults=(None,) * len(LISTING_FIELDS))):
    def id(self):
        """Returns a presumed-unique id for the room in the form of (building id)___(room number)."""
        # /id/1234 or /id/1234/56
        parts = Listing.parselink(self.link)
        if parts is None:
            return None
        building, room = parts
        if room is not None and room != self.roomnumber:
            print("ERROR: Link [%s] doesn't match room number [%s] in item [%s]" %(self.link, self.roomnumber, self))
        return "___".join([building, self.roomnumber])

    @staticmethod
    def fromdict(dict_: Dict[str, Any]):
        args = [dict_.get(k, None) for k in LISTING_FIELDS]
        return Listing(*args)

    @staticmethod
    def parselink(link: str) -> Optional[Tuple[str, Optional[str]]]:
        parts = link.split("/")
        if len(parts) < 3 or len(parts) > 5:
            print("ERROR: Invalid link [%s]" %(link))
            return None
        if len(parts) == 3:
            return parts[2], None
        return parts[2], parts[3]

def NormalizeValue(value: str) -> str:
    normalized = value.strip()
    normalized = re.sub(r"[\s]+", " ", normalized)
    return normalized


class ParsedNumber(recordclass.recordclass("ParsedNumber", ["text", "unit", "value", "parsed"], [None, None, None, False])):
    @staticmethod
    def Parse(text:str, unit: str):
        value = None
        parsed = False
        norm = text
        if norm.endswith(unit):
            norm = norm[:len(norm) - len(unit)]
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
