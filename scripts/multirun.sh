#!/bin/bash

killall pqserver
sleep 1

./obj/pqserver -kl=8077 -H hosts-5.txt -P twitternew -B 1 &> ./obj/out_0.txt &
./obj/pqserver -kl=8078 -H hosts-5.txt -P twitternew -B 1 &> ./obj/out_1.txt &
./obj/pqserver -kl=8079 -H hosts-5.txt -P twitternew -B 1 &> ./obj/out_2.txt &
./obj/pqserver -kl=8080 -H hosts-5.txt -P twitternew -B 1 &> ./obj/out_3.txt &
./obj/pqserver -kl=8081 -H hosts-5.txt -P twitternew -B 1 &> ./obj/out_4.txt &
sleep 1
 
./obj/pqserver -H hosts-5.txt -B 1 --twitternew --verbose --ppost=0 --plogout=0 --psubscribe=0
