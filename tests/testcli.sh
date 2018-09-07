#!/bin/bash
# A simple test case
set -e
KEEP=0
VERBOSE=""
for arg in $*; do
  echo $arg
  if [[ "$arg" == "--keep" ]]; then
    KEEP=1
  fi
  if [[ "$arg" == "--verbose" ]]; then
    VERBOSE="--verbose"
  fi
done
  

SAVEDIR=`pwd`
rm -rf ./test
mkdir ./test
cd ./test
WORKDIR=`pwd`

# create a small git repo
mkdir code
cd code
git init
echo "print('test')" >test.py
git add test.py
git commit -m "initial version"

cd $WORKDIR
dws $VERBOSE init
dws $VERBOSE add git --role=code --name=code ./code
dws $VERBOSE snapshot -m "first version" V1


cd $SAVEDIR
if [[ "$KEEP" == 0 ]]; then
    echo "test cleanup..."
    rm -rf ./test
fi
echo "Test successful."
exit 0
