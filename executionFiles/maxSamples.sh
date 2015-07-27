#!/bin/sh
#$1: app name
#$2: chunk id
#$3: number of samples to execute

sh $name $chunk_id $max_samples &> ${name}_${chunk_id}_output.txt

if [ "$?" -eq "0" ];  then
	echo $max_samples
	exit 0
else
	echo 0
	exit 127
fi

exit 127
