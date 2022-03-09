=========================
Tests for Data Workspaces
=========================

This directory contains the test suite for DWS. To ensure you have all the dependencies, use 
the Anaconda environment specified in dws-test-environment.yml. We use the oldest version of
Python we support to catch any accidental use of newer syntax contructs.

You can use the Makefile to drive the tests. ``make test`` will run the pyflakes checks,
mypy checks, and the unit test suite. Some tests use ``unittest.skipUnless`` to check
for any external requirements and skip the test if the requirements are not satisfied.

A few tests require special configuration:

* The rclone tests need to have the ``rclone`` executable installed and a remote called *localfs*.
  ``rclone`` may be downloaded from https://rclone.org. On Ubuntu-derived Linux, you can install via
  ``sudo apt-get rclone``.
  To configure the remote, create the file ~/.config/rclone/rclone.conf with a section ``localfs`` and a
  single setting: ``type = local``
* ``test_rclone.py`` has additional tests that need an ``rclone`` remote called *dws-test* to be
  configured in the system's rclone configuration file.
* ``test_s3_resource.py`` has tests that require an s3 bucket. This bucket should be specified
  in the file ``test_params.cfg`` is follows::

    [s3_resource]
    s3_bucket=YOUR_BUCKET_NAME


