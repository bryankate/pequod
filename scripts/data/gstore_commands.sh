#!/bin/bash

set -e

DRIVERS=~/pequod/drivers

function reinstall_gstore_pdos() {
  #sudo aptitude install debian-archive-keyring -y
  sudo apt-get update
  sudo apt-get install git autoconf autoconf2.13 \
      protobuf-c-compiler libprotobuf-dev \
      build-essential autoconf libtool git libev-dev libprotoc-dev \
      libprotobuf-dev protobuf-compiler libboost-dev libboost-thread-dev \
      libboost-serialization-dev libboost-program-options-dev \
      libboost-system-dev google-perftools python-protobuf gcc-4.7 -y
  cd ~
  if test -d gstore_exp; then
    (cd gstore_exp; rm -rf *; git fetch origin && git reset --hard origin/master)
  else
    rm -rf gstore_exp
    git clone ssh://am.csail.mit.edu/home/am0/eddietwo/gstore.git gstore_exp
  fi
}

function reinstall_gstore_local() {
  cd ~
  if test -d gstore_exp; then
    (cd gstore_exp; rm -rf *; git fetch origin && git reset --hard origin/master)
  else
    rm -rf gstore_exp
    git clone ssh://am.csail.mit.edu/home/am0/eddietwo/gstore.git gstore_exp
  fi
}

function git_pull_and_make() {
  cd ~/gstore_exp
  git pull
  if ! test -f GNUmakefile ; then
      autoconf
      autoheader
      ./configure
  fi
  make
}

function gokill() {
    killall gstore_server twitter hackernews -q
}

prepare_redis_client() {
  if [ ! -f "$DRIVERS/bin/include/hiredis" ]; then
    mkdir $DRIVERS $DRIVERS/bin -p
    pushd $DRIVERS
    git clone http://github.com/redis/hiredis.git
    cd hiredis
    make
    PREFIX=$DRIVERS/bin make install
    popd
    sudo apt-get install libev-dev --yes
  fi
  pushd $BENCH
  REDIS_ONLY=1 make clean
  REDIS_ONLY=1 make
  popd
  test -f /usr/include/ev++.h || sudo apt-get install libev-dev --yes
}

if test $# -lt 1 ; then
  echo "Usage: $0 [command]"
  exit
fi

$@
