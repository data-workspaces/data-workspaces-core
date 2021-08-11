"""Unit tests related to the S3 resource type.
"""
import os
from os.path import exists, join
import shutil
import gzip
import unittest
import re

from dataworkspaces.resources.s3.snapfs import S3Snapshot
from dataworkspaces.api import get_filesystem_for_resource

from utils_for_tests import TEMPDIR, WS_DIR, write_gzipped_json, get_configuration_for_test, SimpleCase

SNAPSHOT_PATH=join(TEMPDIR, 'snapshot.json.gz')

# We store the bucket name in the test_params.cfg file. If not present,
# we'll skip any tests actually calling s3.
S3_BUCKET_CONFIGURATION=get_configuration_for_test('s3_resource',
                                                   required_properties=['s3_bucket'])

# This is the data from a real snapshot. We save it here so that
# we can have a self-contained test.
SNAPSHOT_DATA = {
  "daily_stats_by_month/2021-03-16_until_2021-04-01_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-04-01_until_2021-05-01_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-05_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-06_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-07_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-08_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-09_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-10_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-11_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-12_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-13_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-14_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-15_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-16_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-17_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-18_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-19_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-20_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-21_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-22_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-23_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-24_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-25_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-26_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-27_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-28_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-29_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-30_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-05-31_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-05-01_until_2021-06-01_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-02_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-03_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-04_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-05_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-06_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-07_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-08_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-09_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-10_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-11_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-12_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-13_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-14_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-15_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-16_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-17_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-18_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-19_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-20_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-21_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-22_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-23_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-24_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-25_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-26_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-27_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-28_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-29_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-06-30_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-06-01_until_2021-07-01_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-07-01_until_2021-07-02_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-07-01_until_2021-07-03_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-07-01_until_2021-07-04_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-07-01_until_2021-07-05_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-07-01_until_2021-07-06_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-07-01_until_2021-07-07_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-07-01_until_2021-07-08_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-07-01_until_2021-07-09_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-07-01_until_2021-07-10_http_requests.json.gz": "null",
  "daily_stats_by_month/2021-07-01_until_2021-07-11_http_requests.json.gz": "9aGE2is_xOd89Wtt7SUREeNpljZ2kebg",
  "daily_stats_by_month/2021-07-01_until_2021-07-12_http_requests.json.gz": "KPnHxNqI0BETVY9LByRLKUiDhd3TEbPu",
  "daily_stats_by_month/2021-07-01_until_2021-07-13_http_requests.json.gz": "JdD6Mw4IuwnnBFBxhJpVdL5F5hJfey0P",
  "daily_stats_by_month/2021-07-01_until_2021-07-14_http_requests.json.gz": "Ewe51zCSasr1a_V4kmk6tsqs0QChtK32",
  "daily_stats_by_month/2021-07-01_until_2021-07-15_http_requests.json.gz": "I6ZCtdQmajFATQP7mxAVMTM2Lv8yqoe9",
  "daily_stats_by_month/2021-07-01_until_2021-07-16_http_requests.json.gz": "gVFlD_CA_iURVicpJ6D4I7LrtoW0bxuj",
  "daily_stats_by_month/2021-07-01_until_2021-07-17_http_requests.json.gz": "zQpDjwMWgd3wqsJpfp7nsM1FCqA0MkkW",
  "daily_stats_by_month/2021-07-01_until_2021-07-18_http_requests.json.gz": "KA.pYcKthPZkjH5fLcGT_Xtbw4J63WJL",
  "daily_stats_by_month/2021-07-01_until_2021-07-19_http_requests.json.gz": "12j_wbzNyjGWwUQJOo9Sp9.pVRVSWBws",
  "daily_stats_by_month/2021-07-01_until_2021-07-20_http_requests.json.gz": "UyEpKh6m_Fb4zGa8PvQ9f_trnWh2XDj7",
  "daily_stats_by_month/2021-07-01_until_2021-07-21_http_requests.json.gz": "GF8X5yS7SzIUht1tPrcC26LaS6QIFDmP",
  "daily_stats_by_month/2021-07-01_until_2021-07-22_http_requests.json.gz": "GoBz1ODflJII6o7F7qRn5pxBK_kSm7iD",
  "daily_stats_by_month/2021-07-01_until_2021-07-23_http_requests.json.gz": "i6fn017lsfrj5.HKp34tmSpfrljSNomI",
  "hourly_stats_by_day/2021-04-22_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-04-23_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-04-24_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-04-25_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-04-26_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-04-27_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-04-28_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-04-29_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-04-30_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-01_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-02_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-03_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-04_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-05_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-06_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-07_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-08_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-09_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-10_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-11_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-12_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-13_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-14_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-15_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-16_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-17_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-18_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-19_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-20_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-21_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-22_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-23_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-24_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-25_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-26_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-27_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-28_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-29_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-30_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-05-31_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-01_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-02_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-03_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-04_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-05_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-06_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-07_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-08_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-09_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-10_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-11_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-12_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-13_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-14_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-15_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-16_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-17_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-18_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-19_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-20_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-21_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-22_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-23_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-24_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-25_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-26_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-27_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-28_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-29_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-06-30_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-07-01_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-07-02_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-07-03_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-07-04_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-07-05_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-07-06_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-07-07_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-07-08_http_requests.json.gz": "null",
  "hourly_stats_by_day/2021-07-09_http_requests.json.gz": "qYw9kSREAlTBYSXJNI5N69WTkGMBtFOE",
  "hourly_stats_by_day/2021-07-10_http_requests.json.gz": "7zshZvlKnBaiDvWB0SiAfAThZhhDccnU",
  "hourly_stats_by_day/2021-07-11_http_requests.json.gz": "cGpfIEeL9In1MTGq5kp_OL7GyM5E0vsz",
  "hourly_stats_by_day/2021-07-12_http_requests.json.gz": "ojeSbQLS_5.2pDGdvO0KIRDlaWOBAUGY",
  "hourly_stats_by_day/2021-07-13_http_requests.json.gz": "aLRVdbCAf8KIaOT.IVS6wETAsCF2fje4",
  "hourly_stats_by_day/2021-07-14_http_requests.json.gz": "jKZNX7vZEErh5YNugZZGgYyhf3Pt_MOu",
  "hourly_stats_by_day/2021-07-15_http_requests.json.gz": "wSugdZDxsqbP36Mv_qjFO7Sv.Uc0ztDD",
  "hourly_stats_by_day/2021-07-16_http_requests.json.gz": "QTJCEmmr7pWISkzD3sU8_kMCt_6C2vrn",
  "hourly_stats_by_day/2021-07-17_http_requests.json.gz": "Z6I5QUiMPtbKP67xMw60LzYHyukhOtFF",
  "hourly_stats_by_day/2021-07-18_http_requests.json.gz": "3ffQ9PcUEjaUG6EVQlqc2MJ._B9Cx3is",
  "hourly_stats_by_day/2021-07-19_http_requests.json.gz": "O4sZUkuM5kTLtzAM0p4F99ElO3Eo4EY5",
  "hourly_stats_by_day/2021-07-20_http_requests.json.gz": "slBLj1vnG1iskW3.rXsChSoN5ZA744uR",
  "hourly_stats_by_day/2021-07-21_http_requests.json.gz": "wOFCcShAmMYFLroFZLUcyNNaxX3kCqyk",
  "hourly_stats_by_day/2021-07-22_http_requests.json.gz": "0k2hns2EIfu3b2WGggzNsVoA95k7XU5P",
  "sampled_logs/2021-04-22_sampled_logs.json.gz": "null",
  "sampled_logs/2021-04-23_sampled_logs.json.gz": "null",
  "sampled_logs/2021-04-24_sampled_logs.json.gz": "null",
  "sampled_logs/2021-04-25_sampled_logs.json.gz": "null",
  "sampled_logs/2021-04-26_sampled_logs.json.gz": "null",
  "sampled_logs/2021-04-27_sampled_logs.json.gz": "null",
  "sampled_logs/2021-04-28_sampled_logs.json.gz": "null",
  "sampled_logs/2021-04-29_sampled_logs.json.gz": "null",
  "sampled_logs/2021-04-30_sampled_logs.json.gz": "null",
  "sampled_logs/2021-05-01_sampled_logs.json.gz": "null",
  "sampled_logs/2021-05-02_sampled_logs.json.gz": "null",
  "sampled_logs/2021-05-03_sampled_logs.json.gz": "null",
  "sampled_logs/2021-05-04_sampled_logs.json.gz": "null",
  "sampled_logs/2021-07-09_sampled_logs.json.gz": "i2JEgUIKNN03InKCHm8AbFCJjqb8GZds",
  "sampled_logs/2021-07-10_sampled_logs.json.gz": "18OWJNWICW7N1o4Q7ibKmFPVvcYOn467",
  "sampled_logs/2021-07-11_sampled_logs.json.gz": "VjwrUCZ05G3NEfYkfQzCDDMEvKoOhnsL",
  "sampled_logs/2021-07-12_sampled_logs.json.gz": "viI269ST5kPvhcd8jI4gtR.xk6PmimkD",
  "sampled_logs/2021-07-13_sampled_logs.json.gz": "8Srf4DI635b7FtnqkCYe_vS6js58QYiN",
  "sampled_logs/2021-07-14_sampled_logs.json.gz": "x0L6H3blTmTRB5pNVGsq8kDPqznLGkrd",
  "sampled_logs/2021-07-15_sampled_logs.json.gz": ".eGl8z_.q6DkGezT7ptapeDoaPp0CMrI",
  "sampled_logs/2021-07-16_sampled_logs.json.gz": "woOi0SlHgxJF6ax3DB_Jq.tffv1zDupz",
  "sampled_logs/2021-07-17_sampled_logs.json.gz": "cwD0dJKdAwu9yB5hxZU.YyBid1RzvElm",
  "sampled_logs/2021-07-18_sampled_logs.json.gz": "YJBnJU9xr2oQYS_xSYoBePstmAM_v_.x",
  "sampled_logs/2021-07-19_sampled_logs.json.gz": "3xHlMKhEytsU0HDeRHXZaeaLqijb_01N",
  "sampled_logs/2021-07-20_sampled_logs.json.gz": "CMU_c3__PiFNUm0gMFcyZqMUGwO4zUDB",
  "sampled_logs/2021-07-21_sampled_logs.json.gz": "u2qNFAEAuwFKLsrBUjdLNV7mtMxY4PoS",
  "sampled_logs/2021-07-22_sampled_logs.json.gz": "40r9.6LWZlhBuuBlWYZz0OUvXGvK2jQm",
  "sampled_logs/test/F1.large.jpg": "C3DhKu_KQwSiqXPbVZDOsmHM96ohF8ne"
}

class TestS3SnapFs(unittest.TestCase):
    """The snapfs module deals with building a file-tree like abstraction over
    the snapshot json data. We test with the above data captured from a
    real snapshot.
    """
    def setUp(self):
        if exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)
        os.mkdir(TEMPDIR)
        write_gzipped_json(SNAPSHOT_DATA, SNAPSHOT_PATH)

    def tearDown(self):
        if exists(TEMPDIR):
            shutil.rmtree(TEMPDIR)

    def test_deep_path(self):
        """Test the snapfs api with a single, deeply nested file"""
        data = {'this/is/a/deep/path/foo.json.gz':'snapshot_hash'}
        snapshot = S3Snapshot(data)
        files = snapshot.ls('')
        self.assertEqual(['this'], files)
        files = snapshot.ls('this')
        self.assertEqual(['this/is'], files)
        files = snapshot.ls('this/is')
        self.assertEqual(['this/is/a'], files)
        files = snapshot.ls('this/is/a')
        self.assertEqual(['this/is/a/deep'], files)
        files = snapshot.ls('this/is/a/deep')
        self.assertEqual(['this/is/a/deep/path'], files)
        files = snapshot.ls('this/is/a/deep/path')
        self.assertEqual(['this/is/a/deep/path/foo.json.gz'], files)
        files = snapshot.ls('this/is/a/deep/path/foo.json.gz')
        self.assertEqual(['this/is/a/deep/path/foo.json.gz'], files)

        self.assertFalse(snapshot.exists(''))
        self.assertFalse(snapshot.exists('boo'))
        self.assertTrue(snapshot.exists('this'))
        self.assertFalse(snapshot.exists('this/isnot'))
        self.assertTrue(snapshot.exists('this/is'))
        self.assertTrue(snapshot.exists('this/is/a/deep/path/foo.json.gz'))
        self.assertFalse(snapshot.exists('this/is/a/deep/path/bar.json.gz'))

        self.assertFalse(snapshot.isfile(''))
        self.assertFalse(snapshot.isfile('this'))
        self.assertFalse(snapshot.isfile('this/is/a/deep/path'))
        self.assertTrue(snapshot.isfile('this/is/a/deep/path/foo.json.gz'))
        self.assertFalse(snapshot.isfile('this/is/a/deep/path/bar.json.gz'))

        self.assertEqual('snapshot_hash', snapshot.version_id('this/is/a/deep/path/foo.json.gz'))


    def test_with_snapshot_data(self):
        snapshot = S3Snapshot.read_snapshot_from_file(SNAPSHOT_PATH)
        root_files = snapshot.ls('')
        self.assertEqual(['daily_stats_by_month', 'hourly_stats_by_day', 'sampled_logs'],
                         root_files)
        by_day_files = snapshot.ls('hourly_stats_by_day')
        self.assertEqual(92, len(by_day_files))
        regexp = re.compile(r'^hourly_stats_by_day\/\d\d\d\d-\d\d-\d\d_http_requests\.json\.gz$')
        for filepath in by_day_files:
            self.assertTrue(regexp.match(filepath) is not None)
        filepath = snapshot.ls("hourly_stats_by_day/2021-07-07_http_requests.json.gz")
        self.assertEqual(['hourly_stats_by_day/2021-07-07_http_requests.json.gz'], filepath)

        self.assertTrue(snapshot.exists("hourly_stats_by_day"))
        self.assertFalse(snapshot.isfile("hourly_stats_by_day"))
        self.assertTrue(snapshot.isfile("hourly_stats_by_day/2021-07-07_http_requests.json.gz"))
        self.assertTrue(snapshot.isfile("hourly_stats_by_day/2021-07-07_http_requests.json.gz"))

        # Note: We use the string "null"", as this is returned by the S3 API if versioning was not
        # enabled when the file was added to the bucket. If you specify None instead, you get
        # the latest version of the file!
        self.assertEqual(snapshot.version_id("hourly_stats_by_day/2021-07-07_http_requests.json.gz"),
                         "null")
        self.assertEqual(snapshot.version_id("hourly_stats_by_day/2021-07-16_http_requests.json.gz"),
                         "QTJCEmmr7pWISkzD3sU8_kMCt_6C2vrn")

@unittest.skipUnless(S3_BUCKET_CONFIGURATION is not None,
                     "SKIP: S3 bucket not specified in test_params.cfg")
class TestS3Resource(SimpleCase):
    """End-to-end test of s3 resource in a workspace"""

    def _count_bytes_for_files(self, fs, filepath, mapping):
        entries = fs.ls(filepath)
        print(f"_count_bytes({filepath}) => {len(entries)} entries")
        if len(entries)<6:
            print(f"  entries = {repr(entries)}")
        else:
            print(f"  entries = [{', '.join(entries[0:5])}...]")
        for entry in entries:
            if fs.isfile(entry):
                with fs.open(entry, 'rb') as f:
                    mapping[entry] = len(f.read())
                    #print(f"{entry} => {mapping[entry]}")
            elif fs.isdir(entry):
                assert entry!=filepath, f"Got same path {entry} as a subdirectory"
                self._count_bytes_for_files(fs, entry, mapping)
            else:
                self.fail(f"Invalid entry {entry}")


    def test_s3_resource(self):
        """We build a repo with an s3 resource, and then walk the resource's tree,
        getting the file sizes of each file. We then take a snapshot, re-read the
        file sizes, and compare the before and after file size dicts"""
        self._setup_initial_repo()
        bucket = S3_BUCKET_CONFIGURATION.get('s3_bucket')
        print(bucket)
        self._run_dws(['add', 's3', '--role', 'source-data', bucket])
        fs = get_filesystem_for_resource(bucket, WS_DIR, verbose=True)
        pre_snapshot_entries = {}
        self._count_bytes_for_files(fs, "", pre_snapshot_entries)
        self._run_dws(['snapshot', 'TAG1'])
        post_snapshot_entries = {}
        self._count_bytes_for_files(fs, "", post_snapshot_entries)
        self.assertEqual(pre_snapshot_entries, post_snapshot_entries)

if __name__ == '__main__':
    unittest.main()
