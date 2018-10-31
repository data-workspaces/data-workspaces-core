#!/bin/bash
# A simple test case
set -e
KEEP=0
VERBOSE=""
BATCH=""
for arg in $*; do
  echo $arg
  if [[ "$arg" == "--keep" ]]; then
    KEEP=1
  fi
  if [[ "$arg" == "--verbose" ]]; then
    VERBOSE="--verbose"
  fi
  if [[ "$arg" == "--batch" ]]; then
    BATCH="--batch"
  fi
done
  

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

# create a small git repo
cd $REMOTE
git init --bare code.git
cd $WORKDIR
mkdir code
cd code
git init
echo "print('test')" >test.py
git add test.py
git commit -m "initial version"
git remote add origin $REMOTE/code.git

# create a local directory structure
cd $WORKDIR
mkdir -p local_files
echo 'File 1' > local_files/f1
echo 'File 2' > local_files/f2
mkdir -p local_files/dir
echo 'File 3' > local_files/dir/f3
ls

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

cd $WORKDIR
dws $VERBOSE init
git remote add origin $REMOTE/test.git
dws $VERBOSE add git --role=code --name=code-git ./code
dws $VERBOSE add local-files --role=code --name=code-local ./local_files
dws $VERBOSE add git --role=results --name=results-git ./results_git
echo dws $VERBOSE snapshot -m "first version" V1
dws $VERBOSE snapshot -m "first version" V1

# make some changes in the git repo
cd code
echo "# Changes" >>test.py
git add test.py
git commit -m "second version"
cd ..

# make some changes to the results
cd results_git
echo "File 3" >f3
echo "File 4" >f4
git add f3 f4
git commit -m "second version"
cd ..

# take a snapshot and then restore to v1
echo dws $VERBOSE snapshot -m "second version" V2
dws $VERBOSE snapshot -m "second version" V2
echo dws $VERBOSE restore V1
dws $VERBOSE $BATCH restore V1

# Restore the git repo back to v2
dws $VERBOSE $BATCH restore --only code-git V2

# Now make a change to the local dir
echo "Removing f1"
rm local_files/f1
# Should fail
dws $VERBOSE $BATCH restore V1 || echo 'Test failed as expected'
echo 'File 1' > local_files/f1

dws $VERBOSE status

echo -n "verify that repo is not dirty..."
git diff --exit-code --quiet
echo "ok"

dws $VERBOSE push

cd $TESTSDIR
if [[ "$KEEP" == 0 ]]; then
    echo "test cleanup..."
    rm -rf ./test
    rm -rf ./remotes
fi
echo "Test successful."
exit 0
