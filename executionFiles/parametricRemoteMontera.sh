#!/bin/sh
#$1: app name
#$2: chunk id
#$3: number of samples to execute


########CONNECT TO REMOTE SERVER AND OBTAIN TASK PARAMETERS
#THEN EXECUTE IT


webServer=gw01.ciemat.es:8080
webServer=localhost:8080

for ((  i = 0 ;  i <= $3 ;  i++  ))
do
  #download input parameter
  inputValue=`curl $webServer/getTask 2>/dev/null`
  #execute App
  sh $name $inputValue > /dev/null
  
  #Error control
  if [ "$?" -ne "0" ];  then
    # mark task as finished
    curl $webServer/finishedTask/$inputValue 2>/dev/null
	echo 0
	exit 127
  fi
  
  #if succesful, mark task as finished
  curl $webServer/finishedTask/$inputValue 2>/dev/null
  
done


#OutputFiles


####EXIT

if [ "$?" -eq "0" ];  then
	echo $max_samples
	exit 0
else
	echo 0
	exit 127
fi

exit 127