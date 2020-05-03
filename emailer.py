from listing import Listing
from typing import List, Set
import os
import traceback
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


class Emailer(object):
    def __init__(self, host, logfile):
        self.host = host
        self.logfile = logfile
        if not os.path.exists(logfile):
            open(logfile, "w")

    def _ReadLog(self) -> List[str]:
        with open(self.logfile) as f:
            return list(f.read().split("\n"))

    def _WriteLog(self, entries: List[str]):
        with open(self.logfile, "w") as f:
            f.write("\n".join(entries))

    def _RenderToHtml(self, listing: Listing) -> str:
        return """
        <h2><a href="https://{host}{link}">{title}</h2>
        <p>{msq} {ldk} {rent} <a href="google.com/maps/place/{address}">{address}</a>
        <br>
        """.format(
            host=self.host,
            link=listing.link,
            title=listing.name,
            msq=listing.msq.text,
            rent=listing.rent.text,
            ldk=listing.ldk,
            address=listing.address,
        )

    def _DoSend(self, contents: List[str]) -> bool:
        if not contents:
            return True
        contents = ["<strong>いい物件が%d件見つかったけん！</strong>" % len(contents)] + contents
        message = Mail(
            from_email="tomigaya@example.com",
            to_emails="omindek@gmail.com",
            subject="ウホッ！いい物件",
            html_content="<br>".join(contents),
        )
        try:
            api_key = os.environ.get("SENDGRID_API_KEY")
            print(api_key)
            sg = SendGridAPIClient(api_key)
            response = sg.send(message)
            print(response.status_code)
            print(response.body)
            print(response.headers)
            return True
        except Exception:
            print(traceback.format_exc())
        return False

    def MaybeSend(self, listings: List[Listing]) -> int:
        log = self._ReadLog()
        log_uniq = set(log)
        contents: List[str] = []
        for listing in listings:
            id = listing.id()
            if id in log_uniq:
                print("Email already sent for id %s" % id)
                continue
            print("Email not yet sent for id %s" % id)
            contents.append(self._RenderToHtml(listing))
            log.append(id)
        if self._DoSend(contents):
            self._WriteLog(sorted(log))
        return len(contents)
