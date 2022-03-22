#!/bin/bash
#SBATCH --output=/home/ma/g/griesshaber/logtest.txt

I=0
while :
do
	I=$((I+1))
	echo $I
	sleep 1
done
