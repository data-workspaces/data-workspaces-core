"""
Various pre-defined regular expressions useful for input validation
"""

import re

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
    print("tests passed.")
