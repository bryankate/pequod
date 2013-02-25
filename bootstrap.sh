#! /bin/sh

if [ ! -f tamer/boostrap.sh ]; then
    git submodule init
    git submodule update
fi

{ cd tamer; ./bootstrap.sh; }

autoreconf -i
