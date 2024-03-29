# Change Log
This file is to document DWS releases and the noteable changes in each release.
We started keeping this log as of release 1.5.

## [1.6.0] - 2022-03-13

We are happy to announce release 1.6.0. The primary changes are:

- Added support for Python 3.10 and dropped Python 3.6
- Fixed issue #79 - If the filename of a modified file contains spaces, dws snapshot fails
- Updated to latest version of repo2docker (2022.02.0.1).

## [1.5.2] - 2021-10-11

The dependencies used for the dws deploy commands have been moved out of the default install. They can be included by enabling the docker extra as follows:
```
pip install --upgrade dataworkspaces[docker]
```

## [1.5.1] - 2021-08-11

Minor release to address some mypy and setuptools compatibility issues. Should not
impact users installing via pip.

### Fixed

- Issue [#78](https://github.com/data-workspaces/data-workspaces-core/issues/78) - a workaround for
  an older mypy issue broke the packaging. Ran into a bunch of mypy issues (on Linux only!), which
  were finally resolved by moving the mypy configuration out of pyproject.toml and adding typing
  ignore comments for external package imports in the dataworkspaces.kits submodules.


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


