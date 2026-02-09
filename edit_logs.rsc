/system logging action
set [find name=disk] disk-file-count=10 disk-file-name=Hashlog
/system logging
set [ find where action="changelog" and topics="warning" ] topics=warning,!dhcp
set [ find prefix="hash" ] disabled=no