#!/bin/bash

export LD_LIBRARY_PATH="/home/kester/local/lib64:/home/kester/local/lib:$LD_LIBRARY_PATH"
export LDFLAGS="-L/home/kester/local/lib64 -L/home/kester/local/lib -L/home/kester/source/boost_1_53_0/libs"
export CPPFLAGS="-I/home/kester/local/include -I/home/kester/source/boost_1_53_0"
export PATH="/home/kester/local/bin:$PATH"
