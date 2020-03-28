# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import os
import tempfile
import hashlib

from typing import Dict, Optional, List, Tuple, Iterable, Callable

assert Dict

from dataworkspaces.utils.subprocess_utils import call_subprocess
from dataworkspaces.utils.git_utils import GIT_EXE_PATH
from dataworkspaces.utils.file_utils import safe_rename

BUF_SIZE = 65536  # read stuff in 64kb chunks


def compute_hash(tmpname: str) -> str:
    sha1 = hashlib.sha1()
    with open(tmpname, "rb") as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


def compute_size(fname: str) -> str:
    if not (os.path.exists(fname)):
        raise Exception("File %s does not exist" % fname)
    statinfo = os.stat(fname)
    return str(statinfo.st_size)


BLOB = "blob"
TREE = "tree"
TYPES = (BLOB, TREE)


class HashEntry:
    def __init__(self, name: str, sha: Optional[str]):
        self.name = name
        self.sha = sha


class HashBlob(HashEntry):
    type = BLOB

    def __init__(self, name: str, sha: str):
        super().__init__(name, sha)


class HashTree(HashEntry):
    type = TREE

    def __init__(self, rootdir: str, name: str, add_to_git: bool = True):
        super().__init__(name, None)
        self.path = rootdir
        self.cache = []  # type: List[Tuple[str,str,str]]
        self.add_to_git = add_to_git

    def _index_by_name(self, name):
        for i, t in enumerate(self.cache):
            if t[2] == name:
                return i
        return -1

    _map_id_to_type = {
        BLOB: HashBlob
        # HashTree is added at the end of the class
    }  # type: Dict[str, Callable]

    def _iter_convert_to_object(self, iterable: Iterable[Tuple[str, str, str]]):
        for sha, mode, name in iterable:
            try:
                yield self._map_id_to_type[mode](sha, mode, name)
            except KeyError:
                raise TypeError("Unknown mode %s found in tree data for path '%s'" % (mode, name))

    def trees(self):
        """return the list of direct subtrees"""
        return [i for i in self if i.type == TREE]

    def blobs(self):
        """return the list of file hashes in this directory"""
        return [i for i in self if i.type == BLOB]

    def add(self, name: str, mode: str, sha: str, force: bool = False):
        """Add elements to a tree. 
           :name: name of the file
           :mode: blob or tree
           :sha:  hash
           :force: if true, overwrite previous entry
        """
        item = (sha, mode, name)
        index = self._index_by_name(name)
        if index == -1:
            # this name is not in the cache
            self.cache.append(item)
        else:
            if force:
                self.cache[index] = item
            else:
                prev_item = self.cache[index]
                if prev_item[0] != sha or prev_item[1] != mode:
                    raise ValueError("Item %r existed with different properties" % name)

    def __delitem__(self, name):
        """Deletes an item with the given name if it exists"""
        index = self._index_by_name(name)
        if index > -1:
            del self.cache[index]

    def sort(self):
        """sort the cache in alphabetical order"""
        self.cache.sort(key=(lambda t: t[2]))

    def write(self):
        self.sort()
        # write to a temp file
        fd, tmpname = tempfile.mkstemp()
        with os.fdopen(fd, "w") as f:
            for (mode, sha, name) in self.cache:
                f.write("{}\t{}\t{}\n".format(mode, sha, name))
        # hash the file
        self.hash = compute_hash(tmpname)
        # that is the name of the file
        objfile = os.path.join(self.path, self.hash)
        os.chmod(tmpname, int("755", 8))
        safe_rename(tmpname, objfile)
        if self.add_to_git:
            call_subprocess([GIT_EXE_PATH, "add", self.hash], cwd=self.path, verbose=False)
        return self.hash

    # List protocol
    def __getslice__(self, i, j):
        return list(self._iter_convert_to_object(self.cache[i:j]))

    def __iter__(self):
        return self._iter_convert_to_object(self.cache)

    def __len__(self):
        return len(self.cache)

    def __getitem__(self, item):
        if isinstance(item, int):
            info = self.cache[item]
            return self._map_id_to_type[info[1]](info[0], info[1], info[2])
        raise TypeError("Invalid index type: %r" % item)

    def __contains__(self, item):
        if isinstance(item, HashEntry):
            for info in self.cache:
                if item.sha == info[0]:
                    return True
        return False

    def __reversed__(self):
        return reversed(self._iter_convert_to_object(self.cache))


HashTree._map_id_to_type[TREE] = HashTree


def generate_hashes(
    path_where_hashes_are_stored: str,
    local_dir: str,
    ignore: List[str] = [],
    hash_fun: Callable[[str], str] = compute_hash,
    add_to_git: bool = True,
    verbose: bool = False,
) -> str:
    """traverse a directory tree rooted at :local_dir: and construct the tree hashes
       in the directory :path_where_hashes_are_stored:
       skip directories in :ignore:"""
    hashtbl = {}  # type: Dict[str, str]
    for root, dirs, files in os.walk(local_dir, topdown=False):
        if os.path.basename(root) in ignore:
            if verbose:
                print("skipping %s" % root)
            continue
        if verbose:
            print("generate_hashes: walk at %s" % root)
            print("  files: %s" % ", ".join(files))
            print("  dirs: %s" % ", ".join(dirs))
        t = HashTree(path_where_hashes_are_stored, root, add_to_git=add_to_git)
        for f in files:
            # print(f)
            sha = hash_fun(os.path.join(root, f))
            t.add(f, BLOB, sha)
        for dir in dirs:
            # print(dir)
            if dir in ignore:
                if verbose:
                    print("skipping dir %s under %s" % (dir, root))
                continue
            dirsha = hashtbl[os.path.join(root, dir)]
            t.add(dir, TREE, dirsha)
        h = t.write()
        hashtbl[root] = h
    return hashtbl[local_dir].strip()


def _get_next_element(
    dl: List[str], startindex: int, ignore: List[str], verbose: bool = False
) -> Tuple[Optional[str], int]:
    index = startindex
    for d in dl[startindex:]:
        index = index + 1
        if d in ignore:
            if verbose:
                print("_get_next_element: skipping %s" % d)
            continue
        return d, index
    return None, -1


def check_hashes(
    roothash: str,
    basedir_where_hashes_are_stored: str,
    local_dir: str,
    ignore: List[str] = [],
    hash_fun: Callable[[str], str] = compute_hash,
    verbose: bool = False,
) -> bool:
    """Traverse a directory tree rooted at :local_dir: and check that the files
       match the hashes kept in :basedir_where_hashes_are_stored: and that no new
       files have been added.
       Ignore directories in :ignore:"""
    hashfile = os.path.abspath(os.path.join(basedir_where_hashes_are_stored, roothash))
    if verbose:
        print("Checking hashes. Root hash ", roothash, " root hashfile ", hashfile)

    hashtbl = {os.path.abspath(local_dir): hashfile}
    for root, dirs, files in os.walk(local_dir, topdown=True):
        if verbose:
            print("check_hashes: walk at root=%s" % root)
            print("  files: %s" % ", ".join(files))
            print("  dirs: %s" % ", ".join(dirs))
        if os.path.basename(root) in ignore:
            if verbose:
                print("ignoring %s" % root)
            continue
        try:
            try:
                hashfile = hashtbl[os.path.abspath(root)]
            except KeyError as k:
                print("Key Error:", k)
                print("Hashtbl is\n", hashtbl)
                assert 0, "key error in hash table at %s" % root
            try:
                fd = open(hashfile, "r")
            except:
                print("File %s not found or not readable" % hashfile)
                return False

            dirs.sort()
            files.sort()

            f_index = 0
            d_index = 0
            for line in fd.readlines():
                line = line.rstrip("\n")
                h, kind, name = line.split("\t")
                # print("Line from hash file ", h, kind, name)
                if kind == BLOB:
                    f, f_index = _get_next_element(files, f_index, ignore, verbose=verbose)
                    if verbose:
                        print("f = ", f, " name =", name)
                    if f != name or (f is None):
                        print("File mismatch:", f, " and (hash says)", name)
                        return False
                    sha = hash_fun(os.path.join(root, f))
                    if sha != h:
                        print("Hash mismatch for file: ", f, ":", sha, " and (hash says)", h)
                        return False
                elif kind == TREE:
                    d, d_index = _get_next_element(dirs, d_index, ignore, verbose=verbose)
                    if verbose:
                        print("d = ", d, " name = ", name)
                    if (d != name) or (d is None):
                        print("Dir mismatch: ", d, " and (hash says)", name)
                        return False
                    hashtbl[os.path.abspath(os.path.join(root, d))] = os.path.join(
                        basedir_where_hashes_are_stored, h
                    )
                else:
                    assert kind == TREE or kind == BLOB
            # Make sure that there are no extra files at end of list
            f, f_index = _get_next_element(files, f_index, ignore, verbose=verbose)
            if f is not None:
                print(
                    "Hash mismatch for file %s in directory %s: extra file in directory, not in previous hash"
                    % (f, root)
                )
                return False
            d, d_index = _get_next_element(dirs, d_index, ignore, verbose=verbose)
            if d is not None:
                print(
                    "Hash mismatch for subdirectory %s in directory %s: extra subdirectory, not in previous hash"
                    % (d, root)
                )
                return False
        finally:
            fd.close()
    return True


def generate_sha_signature(
    rsrcdir: str, localpath: str, ignore: List[str] = [], verbose: bool = False
) -> str:
    return generate_hashes(
        rsrcdir, localpath, ignore=ignore, hash_fun=compute_hash, verbose=verbose
    )


def check_sha_signature(
    hashval: str, rsrdir: str, localpath: str, ignore: List[str] = [], verbose: bool = False
) -> bool:
    return check_hashes(
        hashval, rsrdir, localpath, ignore=ignore, hash_fun=compute_hash, verbose=verbose
    )


def generate_size_signature(
    rsrcdir: str, localpath: str, ignore: List[str] = [], verbose: bool = False
) -> str:
    return generate_hashes(
        rsrcdir, localpath, ignore=ignore, hash_fun=compute_size, verbose=verbose
    )


def check_size_signature(
    hashval: str, rsrdir: str, localpath: str, ignore: List[str] = [], verbose: bool = False
) -> bool:
    return check_hashes(
        hashval, rsrdir, localpath, ignore=ignore, hash_fun=compute_size, verbose=verbose
    )


# Tests have been moved to tests/test_hashtree.py
