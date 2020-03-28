"""
Utilities related to hashing, particularly for simulating git's hashing
without necessarily requiring git to be installed.
"""

import hashlib
import re

HASH_RE = re.compile(r"^[0-9a-fA-F]+$")


def is_a_git_hash(s):
    return len(s) == 40 and (HASH_RE.match(s) is not None)


MIN_SHORT_HASH_LEN = 6
# short hashes must be lowercase
SHORT_HASH_RE = re.compile(r"^[0-9a-f]+$")


def is_a_shortened_git_hash(s):
    """We can refer to snapshots using the first 6+ characters
    of the hash
    """
    return len(s) >= MIN_SHORT_HASH_LEN and (SHORT_HASH_RE.match(s) is not None)


def hash_file(fpath):
    """Compute the same hash on the file as git would (e.g. via git hash-object).
    The hash is the sha1 digest, but with a header added to the file first:
    the word "blob", followed by a space, followed by the content length,
    followed by a zero byte.
    """
    with open(fpath, "rb") as f:
        data = f.read()
    size = len(data)
    header = ("blob %d" % size).encode("ascii") + b"\0"
    blob = header + data
    m = hashlib.sha1()
    m.update(blob)
    return m.hexdigest()


def hash_bytes(data: bytes):
    """Compute the same hash on bytes as git would (e.g. via git hash-object).
    The hash is the sha1 digest, but with a header added to the bytes first:
    the word "blob", followed by a space, followed by the content length,
    followed by a zero byte.
    """
    assert isinstance(data, bytes)
    size = len(data)
    header = ("blob %d" % size).encode("ascii") + b"\0"
    blob = header + data
    m = hashlib.sha1()
    m.update(blob)
    return m.hexdigest()


def _test(fname):
    hv = hash_file(__file__)
    assert is_a_git_hash(hv), "hv wasn't a git hash"
    short_hash = hv[0:6]
    assert is_a_shortened_git_hash(hv), "%s wasn't a short git hash" % short_hash
    print(hv)


if __name__ == "__main__":
    # testing
    _test(__file__)
