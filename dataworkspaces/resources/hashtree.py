import os
import tempfile
import sys
import hashlib

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

    def __init__(self, rootdir, name):
        self.path = rootdir
        self.name = name
        self.cache = [ ]
        self.sha = None

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


def generate(path_where_hashes_are_stored, local_dir, skip=[]):
    """traverse a directory tree rooted at :local_dir: and construct the tree hashes
       in the directory :path_where_hashes_are_stored:
       skip directories in :skip:"""
    hashtbl = { }
    for root, dirs, files in os.walk(local_dir, topdown=False):
        print('processing ', root)
        if root in skip:
            continue

        t = HashTree(path_where_hashes_are_stored, root)
        for f in files:
            print(f)
            sha = compute_hash(os.path.abspath(f))
            t.add(f, BLOB, sha)
        for dir in dirs:
            print(dir)
            print(skip)
            if os.path.abspath(os.path.join(root,dir)) in skip:
                continue
            dirsha = hashtbl[os.path.abspath(os.path.join(root, dir))]
            t.add(dir, TREE, dirsha)
        h = t.write()
        hashtbl[root] = h

def test():
    # t = HashTree('.', 'root')
    # t.add('f1', BLOB, 'hash1')
    # t.add('f2', BLOB, 'hash2')
    # n = t.write()
    # print('hash = ', n)
    generate('./tmp', '..', skip=['../resources/tmp'])
        
if __name__ == '__main__':
    test()
