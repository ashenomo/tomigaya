from oauth2client import file, client, tools
from googleapiclient.discovery import build
from httplib2 import Http
from listing import Listing
from typing import Any, List, Dict, Optional
import os
import shutil
import stat


SCOPES = "https://www.googleapis.com/auth/spreadsheets"


class SheetsRenderer(object):
  def __init__(self, sheet_id):
    if not os.path.exists('/tmp/token.json'):
        shutil.copy('token.json', '/tmp/token.json')
        os.chmod('/tmp/token.json', stat.S_IRUSR| stat.S_IWUSR| stat.S_IROTH| stat.S_IWOTH)
    ls_result = os.popen("ls -l /tmp")
    print("Contents of /tmp:\n%s" % ls_result.read())
    store = file.Storage('/tmp/token.json')
    creds = store.get()
    if not creds or creds.invalid:
      flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
      creds = tools.run_flow(flow, store)
    self.service = build('sheets', 'v4', http=creds.authorize(Http()))
    self.sheet_id = sheet_id

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

  def ClearSheetReqs(self, rows=1000, columns=25) -> List[Dict[str, Any]]:
    return [{
      'updateCells': {
        'rows': { 'values': [{'userEnteredValue': {'stringValue': ''}}] * columns },
        'fields': '*',
        'start': { 'sheetId': 0, 'rowIndex': 0, 'columnIndex': 0 },
      }
    },
    {
      'autoFill': {
        'useAlternateSeries': False,
        'sourceAndDestination': {
          'source': { 'sheetId': 0, 'startRowIndex': 0, 'startColumnIndex': 0, 'endRowIndex': 1, 'endColumnIndex': columns },
          'dimension': 'ROWS',
          'fillLength': rows,
        }
      }
    }]

  def ExecuteReqs(self, reqs):
    response = self.service.spreadsheets().batchUpdate(
      spreadsheetId=self.sheet_id,
      body={'requests': reqs}).execute()

def main():
  renderer = SheetsRenderer("1KDESi_sl0COPlf3nKGeeNxXfH9j3BBpUq2mlaHZgKgo")
  renderer.ExecuteReqs(renderer.ClearSheetReqs())
  
if __name__ == '__main__':
  main()