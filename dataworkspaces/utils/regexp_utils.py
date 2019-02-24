"""
Various pre-defined regular expressions useful for input validation
"""

import re
import datetime

# Some regexp basic patterns for hosts and IP addresses. These are missingng the
# ^ and $ characters to force a match of the whole string, so we can compose them
# See https://stackoverflow.com/questions/106179/regular-expression-to-match-dns-hostname-or-ip-address
# for regexps and discussion.
IP_ADDRESS_PAT=r"(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])"
HOSTNAME_PAT=r"([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])(\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]{0,61}[a-zA-Z0-9]))*"
HOSTNAME_RE=re.compile('^'+HOSTNAME_PAT+'$')

# usernames
USERNAME_PAT = r'[a-zA-Z][a-zA-Z0-9\-]{0,30}'
USERNAME_RE = re.compile('^'+USERNAME_PAT+'$')
# build up an regexp for rsync urls
FPATH_PAT=r'(?:\/[^\/]+(?:\/[^\/]+)*\/?)'
#SSH_PAT=r'^(?:(?:'+ OPT_USERNAME_PAT + HOSTNAME_PAT + ':' + FPATH_PAT + ')|(?:' + HOSTNAME_PAT + ':\/)' + ')$'
RSYNC_PAT=r'^(?:(?:' +HOSTNAME_PAT + ':' + FPATH_PAT + ')|(?:' + \
           HOSTNAME_PAT + ':\/)' + ')$'
RSYNC_RE=re.compile(RSYNC_PAT)
FPATH_RE=re.compile('^'+FPATH_PAT+'$')

# Parsing for iso timestamps
# (it was added to the standard library in 3.7, but we want to support 3.5+)
# E.g. 2019-02-22T08:50:24.684124 for no tz
# TODO: support 2019-02-22T09:20:21.154501-08:00 with tz
DT_RE = re.compile(r'^(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)\.(\d+)$')

def isots_to_dt(iso_ts):
    mo = DT_RE.match(iso_ts)
    if mo is None:
        raise TypeError("String '%s' is not a valid iso timestamp" % iso_ts)
    return datetime.datetime(int(mo.group(1)), int(mo.group(2)),
                             int(mo.group(3)),
                             int(mo.group(4)), int(mo.group(5)),
                             int(mo.group(6)), int(mo.group(7)))

# sanity tests
if __name__ == '__main__':
    assert HOSTNAME_RE.match('this-host7')
    assert HOSTNAME_RE.match('foo.local')
    assert not HOSTNAME_RE.match('foo..local')
    assert HOSTNAME_RE.match('192.168.1.9')

    assert RSYNC_RE.match('foo:/bar')
    assert RSYNC_RE.match('foo.com:/bar')
    assert RSYNC_RE.match('foo.com:/bar/')
    assert RSYNC_RE.match('foo.com:/bar/bat9.x')
    assert RSYNC_RE.match("foo.com:/")
    assert not RSYNC_RE.match("foo.com")
    assert not RSYNC_RE.match("/this/is/a/test")
    assert not RSYNC_RE.match('file://foo/bar')

    assert USERNAME_RE.match('root')
    assert USERNAME_RE.match('a')
    assert USERNAME_RE.match('adjfklr6-3dj')
    assert not USERNAME_RE.match('1234')
    assert not USERNAME_RE.match('a123456789012345678901234567789012334')

    dt = isots_to_dt("2019-02-22T08:50:24.684124")
    assert dt==datetime.datetime(2019,2,22,8,50,24,684124)
    print(dt)
    print("tests passed.")
