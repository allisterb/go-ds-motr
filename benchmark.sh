#! /bin/bash

set -e

echo Downloading benchmark files....

if [ -f "01 Track01.flac" ]; then
    echo 01 Track01.flac already exists in this folder.
else
   echo  "Downloading 01 Track01.flac..."
   wget --no-check-certificate https://go-ds-motr-bmark.us-southeast-1.linodeobjects.com/01%20Track01.flac
fi
if [ -f "02 Track02.flac" ]; then
    echo 02 Track02.flac already exists in this folder.
else
   echo Downloading 02 Track02.flac...
   wget --no-check-certificate https://go-ds-motr-bmark.us-southeast-1.linodeobjects.com/02%20Track02.flac
fi
if [ -f "03 Track03.flac" ]; then
    echo 03 Track03.flac already exists in this folder.
else
   echo Downloading 03 Track03.flac from IPFS...
   wget --no-check-certificate https://go-ds-motr-bmark.us-southeast-1.linodeobjects.com/03%20Track03.flac
fi
if [ -f "04 Track04.flac" ]; then
    echo 04 Track04.flac already exists in this folder.
else
   echo Downloading 04 Track04.flac from IPFS...
   wget --no-check-certificate https://go-ds-motr-bmark.us-southeast-1.linodeobjects.com/04%20Track04.flac
fi
if [ -f "05 Video 01.webm" ]; then
    echo 05 Video 01.webm already exists in this folder.
else
   echo Downloading 05 Video 01.webm...
   wget --no-check-certificate https://go-ds-motr-bmark.us-southeast-1.linodeobjects.com/05%20Video%2001.webm
fi
if [ -f "06 Video 02.webm" ]; then
    echo 06 Video 02.webm already exists in this folder.
else
   echo Downloading 06 Video 02.webm...
   wget --no-check-certificate https://go-ds-motr-bmark.us-southeast-1.linodeobjects.com/06%20Video%2002.webm
fi