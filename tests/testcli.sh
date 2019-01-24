#!/bin/bash
# A simple test case
# Copyright 2018,2019 by MPI-SWS and Data-ken Research. Licensed under Apache 2.0. See LICENSE.txt.
set -e
KEEP=0
ARGS=""
VERBOSE="no"
for arg in $*; do
  echo $arg
  if [[ "$arg" == "--keep" ]]; then
    KEEP=1
  fi
  if [[ "$arg" == "--verbose" ]]; then
    ARGS="$ARGS --verbose"
    VERBOSE="yes"
  fi
  if [[ "$arg" == "--batch" ]]; then
    ARGS="$ARGS --batch"
  fi
done
  
# Run a command, potentially print it before execution
function run {
  if [[ "$VERBOSE" == "yes" ]]; then
    echo $*
  fi
  $*        
}

function debug {
  if [[ "$VERBOSE" == "yes" ]]; then
    echo $*
  fi
}

TESTSDIR=`pwd`
rm -rf ./test
mkdir ./test
cd ./test
WORKDIR=`pwd`
cd $TESTSDIR
rm -rf ./remotes
mkdir ./remotes # remote origins for our git repos
cd ./remotes
REMOTE=`pwd`
cd $TESTSDIR
rm -rf ./clones
mkdir ./clones # keep other clones of our repos
cd ./clones
CLONES=`pwd`

debug "TESTSDIR=$TESTSDIR"
debug "WORKDIR=$WORKDIR"
debug "REMOTE=$REMOTE"
debug "CLONES=$CLONES"
debug

# create a small git repo
cd $REMOTE
git init --bare code.git
cd $WORKDIR
mkdir code
cd code
git init
echo "print('test')" >test.py
git add test.py
cp $TESTSDIR/transform_data1.py .
cp $TESTSDIR/transform_data2.py .
git add transform_data1.py transform_data2.py
git commit -m "initial version"
git remote add origin $REMOTE/code.git

function assert_file_exists {
    if [ ! -f $1 ]; then
        echo "ERROR: file $1 does not exist"
        exit 1
    fi
}

function assert_file_not_exists {
    if [ -f $1 ]; then
        echo "ERROR: file $1 does exist, but should not."
        exit 1
    fi
}

function assert_dir_not_exists {
    if [ -d $1 ]; then
        echo "ERROR: directory $1 does exist, but should not."
        exit 1
    fi
}

function die {
    echo $1
    exit 1
}

function assert_string_in_file {
    STRING="$1"
    FILENAME=$2
    grep "$STRING" $FILENAME >/dev/null || die "Did not find $STRING in $FILENAME"
}

function assert_string_not_in_file {
    STRING="$1"
    FILENAME=$2
    ! grep "$STRING" $FILENAME >/dev/null || die "Found $STRING in $FILENAME"
}


function assert_workspace_clean {
    pushd $WORKDIR
    num_diffs=`git status --porcelain | wc -l | awk '{$1=$1};1'`
    if [[ "$num_diffs" != "0" ]]; then
        echo "ERROR: workspace's git repo has $num_diffs uncommitted change(s):"
        git status --porcelain
        exit 1
    fi
    popd
}

# create a local directory structure
cd $WORKDIR
mkdir -p local_files
echo 'File 1' > local_files/f1
echo 'File 2' > local_files/f2
mkdir -p local_files/dir
echo 'File 3' > local_files/dir/f3
# data file to be used in lineage tests
cp $TESTSDIR/data.csv $WORKDIR/local_files/data.csv
ls

# create a local directory for storing intermediate data
cd $WORKDIR
mkdir workspace

# create a git repo for storing results
cd $REMOTE
git init --bare results_git.git
cd $WORKDIR
mkdir -p results_git
cd results_git
git init
echo "File 1" >f1
echo "File 2" >f2
git add f1 f2
git commit -m "initial version"
git remote add origin $REMOTE/results_git.git

# create a git repo to serve as the origin for our data workspace
cd $REMOTE
git init --bare test.git

run cd $WORKDIR
run dws $ARGS init
run git remote add origin $REMOTE/test.git
run dws $ARGS add git --role=code --name=code-git ./code
# run dws $ARGS add local-files --role=source-data --name=code-local ./local_files
run dws $ARGS add rclone --role=source-data --name=code-local localfs:./local_files my_local_files
echo "local_files/" >> .gitignore
run dws $ARGS add local-files --role=intermediate-data --name=wspace ./workspace
run dws $ARGS add git --role=results --name=results-git ./results_git

# Add a git subdirectory resource
cd $WORKDIR
mkdir results
echo "This is a git subdirectory resource" >$WORKDIR/results/README.txt
echo "A,B">$WORKDIR/results/results.csv
git add results/README.txt results/results.csv
git commit -m "Added results subdirectory"
run dws $ARGS add git --role=results --name=results ./results

echo dws $ARGS snapshot -m "first version" V1
dws $ARGS snapshot -m "first version" V1
assert_workspace_clean

# check files in git subdirectory
if [ ! -f $WORKDIR/results/README.txt ]; then
    echo "Missing file $WORKDIR/results/README.txt"
    exit 1
fi
if [ -f $WORKDIR/results/results.csv ]; then
    echo "File $WORKDIR/results/results.csv should have been moved to a subdirectory upon snapshot"
    exit 1
fi

# make some changes in the git repo
cd code
echo "# Changes" >>test.py
git add test.py
git commit -m "second version"
cd ..
assert_string_in_file Changes ./code/test.py

# make some changes to the results
cd results_git
echo "File 3" >f3
echo "File 4" >f4
git add f3 f4
git commit -m "second version"
cd ..

# take a snapshot
echo dws $ARGS snapshot -m "second version" V2
dws $ARGS snapshot -m "second version" V2
assert_workspace_clean

# create a V3 and take a snapshot
echo "# V3" >>./code/test.py
# no need to commit, will auto-commit
echo dws $ARGS snapshot -m "third version" V3
dws $ARGS snapshot -m "third version" V3
assert_workspace_clean
assert_string_in_file V3 ./code/test.py

# restore to V1
echo dws $ARGS restore V1
dws $ARGS restore V1
assert_workspace_clean
# verify that we correctly went to the V1 version of test.py
assert_string_not_in_file Changes ./code/test.py
assert_string_not_in_file V3 ./code/test.py

# Restore the git repo back to v3
# Verify fix for issue #4
echo dws $ARGS restore --only code-git V3
dws $ARGS restore --only code-git V3
# verify that we correctly went to the V3 version of test.py
assert_string_in_file Changes ./code/test.py
assert_string_in_file V3 ./code/test.py

# Now make a change to the local dir
echo "Removing f1"
rm my_local_files/f1
# Should fail
run dws $ARGS restore V1 || echo 'Test failed as expected'
echo 'File 1' > my_local_files/f1

run dws $ARGS status

# create a code repo that is on a branch different than master
echo "Creating and initializing code-other-branch repo"
cd $REMOTE
git init --bare code-other-branch.git
cd $WORKDIR
mkdir code-other-branch
cd code-other-branch
git init
echo "print('test')" >test.py
git add test.py
cp $TESTSDIR/transform_data1.py .
cp $TESTSDIR/transform_data2.py .
git add transform_data1.py transform_data2.py
git commit -m "initial version"
git remote add origin $REMOTE/code-other-branch.git
git push origin master
git branch other-branch
git checkout other-branch
echo "print('other branch')" >>test.py
git add test.py
git commit -m "add a commit to other branch"
git push origin other-branch
git checkout master
cd $WORKDIR
run dws $ARGS add git --role=code --branch=other-branch ./code-other-branch

# test the fix for issue #1 - when moving files to a subdirectory during
# a snapshot, we should be able to handle the case where the files are not
# tracked by git (that should actually normally be the case).
HOSTNAME=`hostname -s`
# git dirty checking will complain about untracked files...
echo "Testing fix for issue #1..."
cd $WORKDIR/results_git
echo "this is a test of an untracked file" >untracked.txt
mkdir untracked_dir
echo "this is another test of an untracked file" >untracked_dir/untracked2.txt
echo "this is a test of a tracked file">tracked.txt
git add tracked.txt
git commit -m "test for issue #1 - add a tracked file"
assert_file_exists untracked.txt
assert_file_exists untracked_dir/untracked2.txt
assert_file_exists tracked.txt
echo dws $ARGS snapshot issue1-test
dws $ARGS snapshot issue1-test
assert_file_not_exists untracked.txt
assert_file_not_exists untracked_dir/untracked2.txt
assert_dir_not_exists untracked_dir
assert_file_not_exists tracked.txt
assert_file_exists snapshots/$HOSTNAME-issue1-test/untracked.txt
assert_file_exists snapshots/$HOSTNAME-issue1-test/untracked_dir/untracked2.txt
assert_file_exists snapshots/$HOSTNAME-issue1-test/tracked.txt
echo "Fix for issue #1 works."

# Test the git-subdirectory resource types, including non-results
# and results.
cd $WORKDIR
mkdir code_subdir
echo "Code git-subdirectory resource" >code_subdir/readme.txt
mkdir results_subdir
echo "Results git-subdirectory resource" >results_subdir/README.txt
git add code_subdir/readme.txt results_subdir/README.txt
git commit -m "Add subdirectories for git subdir resources"
run dws $ARGS add git --role=code --name=code-subdir ./code_subdir
run dws $ARGS add git --role=results --name=results-subdir ./results_subdir
echo "print('v1')" >code_subdir/test.py
echo '{"accuracy":0.95}' >results_subdir/results.json
echo dws $ARGS snapshot -m "Initial snapshot of subdir resources" subdir-v1
dws $ARGS snapshot -m "Initial snapshot of subdir resources" subdir-v1
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v1/results.json
assert_string_in_file v1 code_subdir/test.py
echo "print('v2')" >code_subdir/test.py
echo '{"accuracy":0.96}' >results_subdir/results.json
echo dws $ARGS snapshot -m "Second snapshot of subdir resources" subdir-v2
dws $ARGS snapshot -m "Second snapshot of subdir resources" subdir-v2
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v1/results.json
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v2/results.json
assert_string_in_file v2 code_subdir/test.py
run dws $ARGS restore subdir-v1
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v1/results.json
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v2/results.json
assert_string_in_file v1 code_subdir/test.py
echo "print('v3')" >code_subdir/test3.py
echo '{"accuracy":0.97}' >results_subdir/results.json
echo dws $ARGS snapshot -m "Third snapshot of subdir resources" subdir-v3
dws $ARGS snapshot -m "Third snapshot of subdir resources" subdir-v3
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v1/results.json
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v2/results.json
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v3/results.json
assert_string_in_file v1 code_subdir/test.py
run dws $ARGS restore subdir-v2
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v1/results.json
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v2/results.json
assert_file_exists results_subdir/snapshots/$HOSTNAME-subdir-v3/results.json
assert_file_not_exists code_subdir/test3.py
assert_string_in_file v2 code_subdir/test.py
echo "Verified git-subdir resource functionality"

run dws $ARGS push
# create a clone of code and make some updates
cd $CLONES
git clone $REMOTE/code.git
cd ./code
echo "this is a test" >README.txt
git add README.txt
git commit -m 'added readme'
git push origin master

run cd $WORKDIR
run dws $ARGS pull

run cd $CLONES
run dws $ARGS clone $REMOTE/test.git

# validate the run command and lineage
run cd $WORKDIR
run dws run python code/transform_data1.py my_local_files/data.csv workspace/data1.csv 5
run dws run python code/transform_data2.py workspace/data1.csv results_git/results.csv
run cd ./results_git
run git add results.csv
echo git commit -m "results of lineage1 test"
git commit -m "results of lineage1 test"
run cd ..
echo dws snapshot -m "Test case of lineage commands" LINEAGE1
dws snapshot -m "Test case of lineage commands" LINEAGE1
run dws pull # should invalidate current lineage
if [ -d $WORKDIR/.dataworkspace/current_lineage ]; then
    echo "Current lineage directory $WORKDIR/.dataworkspace/current_lineage not cleared by pull"
    exit 1
else
    echo "current lineage cleared as expected."
fi
run dws run python code/transform_data1.py my_local_files/data.csv workspace/data1.csv 6
run dws run python code/transform_data2.py workspace/data1.csv results_git/results.csv
run cd ./results_git
run git add results.csv
echo git commit -m "results of lineage2 test"
git commit -m "results of lineage2 test"
run cd ..
echo dws snapshot -m "Test case of lineage commands, part 2" LINEAGE2
dws snapshot -m "Test case of lineage commands, part 2" LINEAGE2
dws diff LINEAGE1 LINEAGE2


################# End of Tests ###########
echo -n "verify that dws repo is not dirty..."
run cd $WORKDIR
run git diff --exit-code --quiet
echo "ok"


cd $TESTSDIR
if [[ "$KEEP" == 0 ]]; then
    echo "Cleanup after test..."
    rm -rf ./test
    rm -rf ./remotes
    rm -rf ./clones
fi
echo "Test successful."
exit 0
