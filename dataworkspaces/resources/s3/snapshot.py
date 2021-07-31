
import boto3 # type: ignore
from multiprocessing import Process, JoinableQueue, Queue, cpu_count
import time
import json
import argparse
import sys
import gzip
from os.path import join

from dataworkspaces.utils.hash_utils import hash_bytes

CPU_COUNT=cpu_count()

SNAPSHOTS_SUBDIR=".snapshots"
SNAPSHOTS_PREFIX=".snapshots/" # when getting the list of prefixes from s3, there is a trailing slash

class VersionWorker(Process):
    def __init__(self, work_q, result_q, bucket, max_keys, max_depth):
        super().__init__()
        self.work_q = work_q
        self.result_q = result_q
        self.bucket = bucket
        self.max_keys = max_keys
        self.max_depth = max_depth
        self.versions = {}
        self.client = None

    def get_at_prefix(self, prefix, depth):
        next_key_marker = None
        next_version_id_marker = None
        next_depth = depth + 1
        
        while True:
            kwargs = {'Bucket':self.bucket,
                      'MaxKeys':self.max_keys,
                      'Delimiter':'/',
                      'Prefix':prefix}
            if next_key_marker is not None:
                kwargs['KeyMarker'] = next_key_marker
            if next_version_id_marker is not None:
                kwargs['VersionIdMarker'] = next_version_id_marker
            assert self.client is not None
            resp = self.client.list_object_versions(**kwargs)

            if 'CommonPrefixes'  in resp:
                if next_depth>self.max_depth:
                    for subprefix_dict in resp['CommonPrefixes']:
                        subprefix = subprefix_dict['Prefix']
                        if subprefix==SNAPSHOTS_PREFIX:
                            continue
                        self.get_at_prefix(subprefix, next_depth)
                else:
                    for subprefix_dict in resp['CommonPrefixes']:
                        subprefix = subprefix_dict['Prefix']
                        if subprefix==SNAPSHOTS_PREFIX:
                            continue                        
                        self.work_q.put((subprefix, next_depth))

            # get the files
            if 'Versions' in resp:
                entries = resp['Versions']
                for entry in entries:
                    key = entry['Key']
                    if entry['IsLatest'] and key!=prefix:
                        # self.versions[key] = (entry['LastModified'].isoformat(),
                        #                       entry['VersionId'])
                        self.versions[key] = entry['VersionId']
            truncated = resp['IsTruncated']
            if truncated:
                next_key_marker = resp['NextKeyMarker']
                #new_version_id_marker = resp['NextVersionIdMarker']
            else:
                break

    def run(self):
        self.client = boto3.client('s3')
        while True:
            prefix, depth = self.work_q.get()
            if prefix==None:
                self.result_q.put(self.versions)
                break # stop the thread
            self.get_at_prefix(prefix, depth)
            self.work_q.task_done()

def snapshot_multiprocess(bucket, snapshot_dir, max_keys=1000, num_workers=CPU_COUNT, max_depth=2, dry_run=False):
    """Compute the snapshot and store as a hash in the specified directory.
    The filename will be HASH.json.gz. The hashing occurs before compresssing.
    Returns the hashcode and the versions directory."""
    start = time.time()
    work_q = JoinableQueue() # type: JoinableQueue
    work_q.put(('', 0))
    result_q = Queue() # type: Queue
    workers = [VersionWorker(work_q, result_q, bucket, max_keys, max_depth) for i in range(num_workers)]
    for worker in workers:
        worker.start()
    work_q.join()
    for worker in workers:
        work_q.put((None,None),)

    # combine and write the result
    pre_write= time.time()
    versions = {}
    for i in range(num_workers):
        versions.update(result_q.get())
    #sorted_versions = dict(sorted(versions.items()))
    #with open(snapshot_file, 'w') as f:
    #    json.dump(sorted_versions, f, indent=2)
    data = json.dumps(versions, sort_keys=True, indent=2).encode('utf-8')
    hashcode = hash_bytes(data)
    local_file = join(snapshot_dir, hashcode+'.json.gz')
    with open(local_file, 'wb') as f:
        f.write(gzip.compress(data))
    if not dry_run:
        import s3fs # type: ignore
        fs = s3fs.S3FileSystem()
        snapshot_path = join(join(bucket, SNAPSHOTS_SUBDIR), f'{hashcode}.json.gz')
        fs.put(local_file, snapshot_path)
        print(f"Uploaded snapshot to s3://{snapshot_path}")
    end = time.time()
    print(f"Completed snapshot of {len(versions)} objects in {round(end-start, 1)} seconds")
    print(f"Time to write (included in total) was {round(end-pre_write, 2)} seconds")
    print(f"hashcode={hashcode}")
    return (hashcode, versions)

# just for testing
def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', default=CPU_COUNT, type=int,
                        help=f"Number of workers, defaults to number of CPUs ({CPU_COUNT})")
    parser.add_argument('--max-keys', default=1000, type=int,
                        help="Maximum keys per request, defaults to 1000")
    parser.add_argument('--max-depth', default=2, type=int,
                        help="Maximum depth before switching to same thread, depth-first.")
    parser.add_argument('--dry-run', default=False, action='store_true',
                        help="If specified, don't upload the snapshot file to the bucket")
    parser.add_argument('bucket', metavar="BUCKET", help="Name of bucket")
    parser.add_argument('snapshot_dir', metavar="SNAPSHOT_DIR",
                        help="Name of directory to store snapshot file")
    args = parser.parse_args(argv)
    snapshot_multiprocess(args.bucket, args.snapshot_dir, num_workers=args.workers,
                          max_keys=args.max_keys, max_depth=args.max_depth, dry_run=args.dry_run)
    return 0

if __name__=='__main__':
    sys.exit(main())
