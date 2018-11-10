#!/bin/bash
# A simple test case
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
run dws $ARGS add local-files --role=source-data --name=code-local ./local_files
run dws $ARGS add local-files --role=intermediate-data --name=worksapce ./workspace
run dws $ARGS add git --role=results --name=results-git ./results_git
echo dws $ARGS snapshot -m "first version" V1
dws $ARGS snapshot -m "first version" V1

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
echo dws $ARGS snapshot -m "second version" V2
dws $ARGS snapshot -m "second version" V2
echo dws $ARGS restore V1
dws $ARGS restore V1

# Restore the git repo back to v2
echo dws $ARGS restore --only code-git V2
dws $ARGS restore --only code-git V2

# Now make a change to the local dir
echo "Removing f1"
rm local_files/f1
# Should fail
run dws $ARGS restore V1 || echo 'Test failed as expected'
echo 'File 1' > local_files/f1

run dws $ARGS status

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
run dws run python code/transform_data1.py local_files/data.csv workspace/data1.csv 5
run dws run python code/transform_data2.py workspace/data1.csv results_git/results.csv
run cd ./results_git
run git add results.csv
echo git commit -m "results of lineage1 test"
git commit -m "results of lineage1 test"
run cd ..
echo dws snapshot -m "Test case of lineage commands" LINEAGE1
dws snapshot -m "Test case of lineage commands" LINEAGE1

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
