"""
Run S3 fs against a snapshot
"""

import sys
import argparse
import json
import gzip

from dataworkspaces.errors import PathError


class Directory:
    __slots__ = ('path', 'entries', 'subdirs')
    def __init__(self, path):
        self.path = path
        self.entries = []
        self.subdirs = {}

    def ensure_subdir(self, name):
        if name in self.subdirs:
            subdir = self.subdirs[name]
            assert isinstance(subdir, Directory)
            return subdir
        else:
            subdir_path = name if self.path=="" else self.path + "/" + name
            subdir = Directory(subdir_path)
            self.subdirs[name] = subdir
            self.entries.append(name)
            return subdir

    def add_file(self, name):
        self.entries.append(name)

    def ls(self, path):
        if path=="" and self.path=="":
            return self.entries
        parts = path.split("/")
        parent = self
        for part in parts[0:-1]:
            if part not in parent.subdirs:
                raise PathError(f"Path {path} not present in snapshot")
            parent = parent.subdirs[part]
        leaf = parts[-1]
        def join(p, name):
            return name if p=="" else p + "/" + name
        if leaf not in parent.entries:
            raise PathError(f"Path {path} not present in snapshot")
        if leaf in parent.subdirs:
            return [join(join(parent.path, leaf), entry) for entry in parent.subdirs[leaf].entries]
        else:
            return [path]

    def exists(self, path): 
        parts = path.split("/")
        parent = self
        for part in parts[0:-1]:
            if part not in parent.subdirs:
                return False
            parent = parent.subdirs[part]
        leaf = parts[-1]
        return leaf in parent.entries

    def isfile(self, path):
        parts = path.split("/")
        parent = self
        for part in parts[0:-1]:
            if part not in parent.subdirs:
                return False
            parent = parent.subdirs[part]
        leaf = parts[-1]
        return (leaf in parent.entries) and (leaf not in parent.subdirs)

    def __repr__(self):
        if len(self.entries)>5:
            separator = ',\n '
        else:
            separator = ', '
        s = separator.join([(entry+'/' if entry in self.subdirs else entry) for entry in self.entries])
        return '[' + s + ']'
                
    def validate(self):
        assert len(self.entries)>0, "Empty directory!"
        unique_entries = set(self.entries)
        assert len(self.entries)==len(unique_entries), f"Not all entries are unique: {self}"
        count = len(self.entries) - len(self.subdirs)
        for key in self.subdirs.keys():
            assert key in self.entries, f"entry {key} in subdirs but not keys: {self}"
            subdir = self.subdirs[key]
            count += subdir.validate()
        return count

        
def build_file_tree(snapshot):
    tree = Directory("")
    for key in snapshot.keys():
        parts = key.split('/')
        parent = tree
        for part in parts[0:-1]:
            parent = parent.ensure_subdir(part)
        parent.add_file(parts[-1])
    return tree


def read_snapshot(filename):
    with open(filename, 'rb') as f:
        raw_data = gzip.decompress(f.read()).decode('utf-8')
    return json.loads(raw_data)

class S3Snapshot:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.root = build_file_tree(snapshot)

    def version_id(self, path):
        if path not in self.snapshot:
            raise PathError(f"Path {path} not present in snapshot")
        return self.snapshot[path]
        # versioning including modification time
        # return self.snapshot[path][1]

    def ls(self, path):
        return self.root.ls(path)

    def isfile(self, path):
        return self.root.isfile(path)

    def exists(self, path):
        return self.root.exists(path)

    def __repr__(self):
        return f'S3Snapshot({len(self.snapshot)} entries)'

    @staticmethod
    def read_snapshot_from_file(filename):
        return S3Snapshot(read_snapshot(filename))


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot_file", metavar="SNAPSHOT_FILE",
                        help="Name of snapshot file to read")
    parser.add_argument("path", metavar="path", nargs="?",
                        help="Name of path for ls, defaults to root")
    args = parser.parse_args(argv)
    s = read_snapshot(args.snapshot_file)
    t = S3Snapshot(s)
    if args.path:
        print(f"exists() => {t.exists(args.path)}")
        print(f"isfile() => {t.isfile(args.path)}")
        if t.isfile(args.path):
            print(f"version_id = {t.version_id(args.path)}")
        print(f"ls('{args.path}') => {t.ls(args.path)}")
    else:
        print(t.ls(""))
    return 0


if __name__=="__main__":
    sys.exit(main())
