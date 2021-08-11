# Change Log
This file is to document DWS releases and the noteable changes in each release.
We started keeping this log as of release 1.5.


## [1.5.0] - 2021-08-11
  
We are happy to announce the 1.5 major release of Data Workspaces.
 
### Added
 
- Support for resources based on S3 bukets. This includes the full snapshotting and
  restore of state (leveraging the S3 bucket versioning feature). 
  See the [S3 Resource](https://data-workspaces-core.readthedocs.io/en/latest/resources.html#s3-resources)
  section in the documentation for details.

### Changed
  
- The packaging of the `dataworkspaces` Python package has been
  modernized to use `pyproject.toml` and `setup.cfg`.
 
### Fixed
 
- Problems related to the use of non-default branches for the DWS Git repository and Git resources have been
  fixed (issues [#75](https://github.com/data-workspaces/data-workspaces-core/issues/75) and
  [#76](https://github.com/data-workspaces/data-workspaces-core/issues/76)). This is needed to support
  GitHub's use of `main` as the default primary branch.
- A performed bug in snapshot capture for *local files* and *rclone* resources has been fixed
  (issues [#69](https://github.com/data-workspaces/data-workspaces-core/issues/69) and
  [#77](https://github.com/data-workspaces/data-workspaces-core/issues/77)).
  Snapshots with these resource types are dramatically faster.


