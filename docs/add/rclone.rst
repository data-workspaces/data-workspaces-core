dws: Adding resources using rclone
==================================

dws add rclone [options] source-repo target-repo

*dws add rclone* adds a remote repository set up using rclone.

We use rclone to set up remote repositories.

Example:
We use rclone config to set up a repository pointing to a local directory:

$ rclone config show
; empty config

$ rclone config create localfs local unc true
Remote config
--------------------
[localfs]
type = local
config_automatic = yes
unc = true
--------------------

Now we use the backend to add a repository to dws

$ dws add rclone --role=source-data my_local_files:/Users/rupak/tmp tmpfiles

This creates a local directory tmpfiles and copies the contents of /Users/rupak/tmp to it.

Similarly, we can make a remote S3 bucket.

$ rclone config
mbk-55-51:docs rupak$ rclone --config=rclone.conf config
Current remotes:

Name                 Type
====                 ====
localfs              local

e) Edit existing remote
n) New remote
d) Delete remote
r) Rename remote
c) Copy remote
s) Set configuration password
q) Quit config
e/n/d/r/c/s/q> n
name> s3bucket
Type of storage to configure.
# Pick choice 4 for S3 and configure the bucket
...
# set configuration parameters

Once the S3 bucket is configured, we can get files from it:

$ dws add rclone --role=source-data s3bucket:mybucket s3files


Configuration Files
-------------------

By default, we use the default configuration file used by rclone. This is the file printed out by:

$ rclone config file

and usually resides in $HOME/.config/rclone/rclone.config

However, you can specify a different configuration file:

$ dws add rclone --config=/path/to/configfile --role=source-data localfs:/Users/rupak/tmp tmpfiles

In this case, make sure the config file you are using has the remote localfs defined.
