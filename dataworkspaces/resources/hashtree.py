# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
import os
import tempfile
import sys
import hashlib

from dataworkspaces.utils.subprocess_utils import call_subprocess
from dataworkspaces.utils.git_utils import GIT_EXE_PATH

BUF_SIZE = 65536  # read stuff in 64kb chunks
def compute_hash(tmpname):
    sha1 = hashlib.sha1()
    with open(tmpname, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)
    return sha1.hexdigest()


BLOB = 'blob'
TREE = 'tree'
TYPES = (BLOB, TREE)
class HashBlob(object):
    type = BLOB
    def __init__(self, name, sha):
        self.name = name
        self.sha = sha


class HashTree(object):
    type = TREE

    def __init__(self, rootdir, name, add_to_git=True):
        self.path = rootdir
        self.name = name
        self.cache = [ ]
        self.sha = None
        self.add_to_git = True

    def _index_by_name(self, name):
        for i, t in enumerate(self.cache):
            if t[2] == name:
                return i
        return -1

    _map_id_to_type = { BLOB : HashBlob
                        # HashTree is added at the end of the class 
                      }

    def _iter_convert_to_object(self, iterable):
        for sha, mode, name in iterable:
            try:
                yield self._map_id_to_type[mode](sha, mode, name)
            except KeyError:
                raise TypeError("Unknown mode %o found in tree data for path '%s'" % (mode, name))

    def trees(self):
        """return the list of direct subtrees"""
        return [i for i in self if i.type == TREE]

    def blobs(self):
        """return the list of file hashes in this directory"""
        return [i for i in self if i.type == BLOB]

    def add(self, name, mode, sha, force=False):
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
                self._cache[index] = item
            else:
                prev_item = self.cache[index]
                if prev_item[0] != sha or prev_item[1] != mode:
                    raise ValueError("Item %r existed with different properties" % name)

    def __delitem__(self, name):
        """Deletes an item with the given name if it exists"""
        index = self._index_by_name(name)
        if index > -1:
            del(self.cache[index])

    def sort(self):
        """sort the cache in alphabetical order"""
        self.cache.sort(key=(lambda t:t[2]))
  
    def write(self):
        self.sort()
        # write to a temp file
        fd, tmpname = tempfile.mkstemp()
        with os.fdopen(fd, 'w') as f:
            for (mode, sha, name) in self.cache:
                f.write('{}\t{}\t{}\n'.format(mode, sha, name))
        # hash the file
        self.hash = compute_hash(tmpname)
        # that is the name of the file
        objfile = os.path.join(self.path, self.hash)
        os.chmod(tmpname, int('755', 8)) 
        os.rename(tmpname, objfile)
        if self.add_to_git:
            call_subprocess([GIT_EXE_PATH, 'add', self.hash],
                            cwd=self.path, verbose=False)
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
        if isinstance(item, IndexObject):
            for info in self.cache:
                if item.sha == info[0]:
                    return True
        return False

    def __reversed__(self):
        return reversed(self._iter_convert_to_object(self._cache))

HashTree._map_id_to_type[TREE] = HashTree


def generate_hashes(path_where_hashes_are_stored, local_dir, ignore=[],
                    add_to_git=True):
    """traverse a directory tree rooted at :local_dir: and construct the tree hashes
       in the directory :path_where_hashes_are_stored:
       skip directories in :ignore:"""
    hashtbl = { }
    for root, dirs, files in os.walk(local_dir, topdown=False):
        # print('processing ', root)
        if os.path.basename(root) in ignore:
            continue

        t = HashTree(path_where_hashes_are_stored, root, add_to_git=add_to_git)
        for f in files:
            print(f)
            sha = compute_hash(os.path.join(root, f))
            t.add(f, BLOB, sha)
        for dir in dirs:
            print(dir)
            if dir in ignore: 
                continue
            dirsha = hashtbl[os.path.join(root, dir)]
            t.add(dir, TREE, dirsha)
        h = t.write()
        hashtbl[root] = h
    return hashtbl[local_dir].strip()

def _get_next_element(dl, startindex, ignore):
    index = startindex
    for d in dl[startindex:]:
        index = index + 1 
        if d in ignore:
            continue
        return d, index
    return None, -1 

def check_hashes(roothash, basedir_where_hashes_are_stored, local_dir, ignore=[]):
    """Traverse a directory tree rooted at :local_dir: and check that the files
       match the hashes kept in :basedir_where_hashes_are_stored: and that no new
       files have been added.
       Ignore directories in :ignore:"""
    hashfile = os.path.abspath(os.path.join(basedir_where_hashes_are_stored, roothash))
    print('Checking hashes. Root hash ', roothash, ' root hashfile ', hashfile)
 
    hashtbl = { os.path.abspath(local_dir) : hashfile }
    for root, dirs, files in os.walk(local_dir, topdown=True):
        print('processing ', root, files, dirs)
        if os.path.basename(root) in ignore:
            continue
        try:
            try:
                hashfile = hashtbl[os.path.abspath(root)]
            except KeyError as k:
                print("Key Error:", k)
                print("Hashtbl is\n", hashtbl)
                sys.exit(1)
            try:
                fd = open(hashfile, 'r') 
            except:
                print('File %s not found or not readable' % hashfile)
                return False

            dirs.sort()
            files.sort()

            f_index = 0
            d_index = 0
            for line in fd.readlines():
                line = line.rstrip('\n')
                h, kind, name = line.split('\t')
                # print("Line from hash file ", h, kind, name)
                if kind == BLOB:
                    f, f_index = _get_next_element(files, f_index, ignore) 
                    print("f = ", f, " name =", name)
                    if f != name:
                        print("File mismatch:", f, " and (hash says)", name)
                        return False
                    sha = compute_hash(os.path.join(root, f))
                    if sha != h:
                        print("Hash mismatch for file: ", f, ":", sha, " and (hash says)", h)
                        return False
                elif kind == TREE:
                    d, d_index = _get_next_element(dirs, d_index, ignore)
                    print("d = ", d, " name = ", name)
                    if d != name:
                        print("Dir mismatch: ", d, " and (hash says)", name)
                        return False
                    hashtbl[os.path.abspath(os.path.join(root, d))] = os.path.join(basedir_where_hashes_are_stored, h)
                else:
                    assert (kind == TREE or kind == BLOB)
        finally:
            fd.close()
    return True

def test_walk(base):
    for root, dir, files in os.walk(base, topdown=True):
        print(root, "\t", dir, "\t", files)
 
def test():
    # t = HashTree('.', 'root')
    # t.add('f1', BLOB, 'hash1')
    # t.add('f2', BLOB, 'hash2')
    # n = t.write()
    # print('hash = ', n)
    h = generate_hashes('./tmp', '..', ignore=['tmp', '__pycache__'])
    # h = generate_hashes('./tmp', '..', ignore=['tmp', '.git'])
    print("Hash of .. is ", h)

    print("\n\nTest")
    test_walk('..')

    print("\n\nChecking hashes (should work)")
    b = check_hashes(h, './tmp', '..', ignore=['tmp', '__pycache__'])
    print(b)

    with open('log', 'w') as f:
        f.write("AHA")
    print("\n\nChecking hashes again (should fail)")
    b = check_hashes(h, './tmp', '..', ignore=['tmp', '__pycache__'])
    print(b)
    os.remove('log')
    
        
if __name__ == '__main__':
    test()
