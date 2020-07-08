from oauth2client import file, client, tools
from googleapiclient.discovery import build
from httplib2 import Http
from listing import Listing
from typing import Any, List, Dict, Optional
import json
import pprint
import os
import shutil
import stat
import jsonpickle


SCOPES = "https://www.googleapis.com/auth/spreadsheets"


class SheetsRenderer(object):
    def __init__(self, spreadsheet_id):
        if not os.path.exists("/tmp/token.json"):
            shutil.copy("token.json", "/tmp/token.json")
            os.chmod(
                "/tmp/token.json",
                stat.S_IRUSR | stat.S_IWUSR | stat.S_IROTH | stat.S_IWOTH,
            )
        ls_result = os.popen("ls -l /tmp")
        print("Contents of /tmp:\n%s" % ls_result.read())
        store = file.Storage("/tmp/token.json")
        creds = store.get()
        if not creds or creds.invalid:
            flow = client.flow_from_clientsecrets("credentials.json", SCOPES)
            creds = tools.run_flow(flow, store)
        self.service = build("sheets", "v4", http=creds.authorize(Http()))
        self.spreadsheet_id = spreadsheet_id
        self.sheet_id = 0
        self.sheet_name = ""

    def CreateAndUseSheet(self, title, rows=3000, cols = 40) -> bool:
        """Returns True if a new sheet was created."""
        sheets = self.ReadSheetList()
        if title in sheets.keys():
            self.sheet_id = sheets[title]
            self.sheet_name = title
            print("Using existing sheet_id %s for %s" % (self.sheet_id, title))
            return False
        reqs: List[Dict[str, Any]] = [dict(
            addSheet=dict(
                properties=dict(
                    title=title, gridProperties=dict(rowCount=rows, columnCount=cols)
                )
            )
        )]
        responses = self.ExecuteReqs(reqs)
        sheet_id = responses[0]["replies"][0]["addSheet"]["properties"]["sheetId"]
        print("CreateAndUseSheet new sheetId: %s, response: %s" % (sheet_id, responses[0]))
        self.sheet_id = sheet_id
        self.sheet_name = title
        return True

    def UpdateCellReq(self, row, col, contents: List[Dict[str, str]]) -> Dict[str, Any]:
        return {
            "updateCells": {
                "rows": {"values": [{"userEnteredValue": x} for x in contents]},
                "fields": "*",
                "start": {"sheetId": self.sheet_id, "rowIndex": row, "columnIndex": col},
            }
        }

    def ClearSheetReqs(self, rows=1000, columns=25) -> List[Dict[str, Any]]:
        return [
            {
                "updateCells": {
                    "rows": {
                        "values": [{"userEnteredValue": {"stringValue": ""}}] * columns
                    },
                    "fields": "*",
                    "start": {"sheetId": self.sheet_id, "rowIndex": 0, "columnIndex": 0},
                }
            },
            {
                "autoFill": {
                    "useAlternateSeries": False,
                    "sourceAndDestination": {
                        "source": {
                            "sheetId": self.sheet_id,
                            "startRowIndex": 0,
                            "startColumnIndex": 0,
                            "endRowIndex": 1,
                            "endColumnIndex": columns,
                        },
                        "dimension": "ROWS",
                        "fillLength": rows,
                    },
                }
            },
        ]

    def ExecuteReqs(self, reqs, batch_size=50):
        def batch(iterable, n):
            l = len(iterable)
            for ndx in range(0, l, n):
                last = min(ndx + n, l)
                print("Processing reqs %5d / %5d" % (last, l))
                yield iterable[ndx:last]
        responses = []
        for slc in batch(reqs, batch_size):
            responses.append(
                self.service.spreadsheets()
                  .batchUpdate(spreadsheetId=self.spreadsheet_id, body={"requests": slc})
                  .execute()
            )
        return responses
    
    def ReadRange(self, range, **kwargs):
        if "!" not in range:
            range = "'%s'!%s" % (self.sheet_name, range)
        result = self.service.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id,
            range=range, **kwargs).execute()
        read_values = result.get('values', [])
        if not read_values:
            return []
        return read_values[0]

    def ReadSheetList(self) -> Dict[str, int]:
        """Returns dict of title --> sheetId."""
        sheets = self.service.spreadsheets().get(
            spreadsheetId=self.spreadsheet_id).execute()["sheets"]
        return {s["properties"]["title"]: s["properties"]["sheetId"] for s in sheets}

    def FindColumn(self, title) -> str:
        def colToExcel(col): # col is 1 based
            excelCol = ""
            div = col 
            while div:
                (div, mod) = divmod(div-1, 26) # will return (x, 0 .. 25)
                excelCol = chr(mod + 65) + excelCol
            return excelCol
        
        col = None
        headers = self.ReadRange("1:1")
        for i, name in enumerate(headers):
            if name == title:
                col = colToExcel(i + 1)
                break
        assert col is not None, "No '%s' header found: [%s]" % (title, headers)
        return col, i

    def ReadPickleDb(self) -> List[Listing]:
        print("Reading PickleDb from %s sheet %d (%s)" % (self.spreadsheet_id, self.sheet_id, self.sheet_name))
        col, _ = self.FindColumn("pickle")
        pickle_values = self.ReadRange("%s2:%s" % (col, col), majorDimension="COLUMNS")
        return [jsonpickle.decode(p) for p in pickle_values]


def main():
    renderer = SheetsRenderer("1KDESi_sl0COPlf3nKGeeNxXfH9j3BBpUq2mlaHZgKgo")
    print(renderer.FindColumn("text"))
    #print(renderer.ReadSheetList())
    #print(renderer.ReadPickleDb())
    """
    result = renderer.service.spreadsheets().values().get(
        spreadsheetId=renderer.spreadsheet_id,
        range="1:1").execute()
    print(result)
    print()
    print(result.get('values', []))
    """


if __name__ == "__main__":
    main()

