#!/bin/bash
# A simple test case
set -e
if [[ "$1" == "--keep" ]]; then
  KEEP=1
else
  KEEP=0
fi

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
dws init
dws add git --role=code ./code
dws snapshot -m "first version" V1


cd $SAVEDIR
if [[ "$KEEP" == 0 ]]; then
    echo "test cleanup..."
    rm -rf ./test
fi
echo "Test successful."
exit 0
