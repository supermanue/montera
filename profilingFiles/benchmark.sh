#!/bin/sh
set -x
#BENCHMARKING SCRIPT

# TODO : quizá sea GW_task_id
output_file="execution_result_"$GW_JOB_ID".xml"

#info about the host
hostname
hostname=$GW_HOSTNAME
echo "<execution_info>" > $output_file
echo "	<type>benchmark</type>" >> $output_file
echo "	<hostname>"$hostname"</hostname>" >> $output_file
echo "------"
uname -a 
echo "-------"

#variables to control execution time
init_date=`date +%s`

#first, analyze system performance
chmod +x whetstone
whetstones=` ./whetstone 500000 | grep Whetstones | awk '{print $6}' `
echo "	<whetstones>"$whetstones"</whetstones>" >>  $output_file

end_date=`date +%s`

echo "	<execution_time>"`expr $end_date - $init_date `"</execution_time>" >>  $output_file

#store size of the transfered files, to calculate bandwith
data_size=`du -s | awk '{print $1}'`
echo "	<data_size>"$data_size"</data_size>" >> $output_file
echo "</execution_info>" >> $output_file


#if myVar exists, it is neccesary to update its value

if [ -n "$PILOT_u5682_VAR_5" ]; then
	echo "float: " $whetstones 
	whetstonesInt=${whetstones%.*}
	echo $whetstonesInt > $PILOT_u5682_VAR_5
fi 


