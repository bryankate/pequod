#!/bin/bash

sudo service redis-server stop
sudo umount /run/shm
sudo mount -t tmpfs /dev/shm /run/shm
mkdir /run/shm/redislog -p

conf="
appendonly yes
port 6379
dir /run/shm/redislog
include `pwd`/common.conf"

echo "$conf" > .redis_one.conf

killall redis-server
redis-server .redis_one.conf
