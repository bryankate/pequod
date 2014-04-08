#! /bin/sh

# check to see if we have the submodules checked out
if [ ! -f tamer/boostrap.sh ] || [ ! -f memtier_benchmark/README.md ]; then
    git submodule init
    git submodule update
fi

( cd tamer; ./bootstrap.sh )

autoreconf -i
