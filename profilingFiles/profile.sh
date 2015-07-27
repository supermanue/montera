#!/bin/sh
set -x
#PROFILING SCRIPT
#FIRST PARAMETER: SRIPT OF THE APP TO PROFILE
#SECOND PARAMETER: MAX PROFILING TIME
#THIRD PARAMETER: name of the profiling

#info about the host
hostname
echo "------"
uname -a 
echo "-------"

#variables to control execution time
init_date=`date +%s`
end_date=`expr $init_date + $2`

#first, analyze system performance
chmod +x whetstone
./whetstone 500000 > benchmark_$3

aux_date=`date +%s`
echo "with LOOP=500000, whetstone execution took" `expr $aux_date - $init_date `" seconds"

#then, run application as many times as posible within the authorized time
#first param: number of task
#second param: number of particles
echo "num_parts exec_time" >> benchmark_$3

num_parts=1

echo "number_of_parts	time"
echo "<results>" > $3

while  [[ `date +%s` -le $end_date ]] 
do
	init_date=`date +%s`

	echo "" >> applicationOutput_$GW_JOB_ID.txt
	echo "Executiin app with $num_parts simulations" >> applicationOutput_$GW_JOB_ID.txt
	echo ""  >> applicationOutput_$GW_JOB_ID.txt

	sh $1 0 $num_parts >> applicationOutput_$GW_JOB_ID.txt #y ejecucion de la aplicacion
	
	#Error control
	rc=$?
	if [[ $rc != 0 ]] ; then
	    echo "Execution failed when profiling, exiting..."
	    exit $rc
	fi

 	aux_date=`date +%s`	

	echo "	<profile>" >> $3 
	echo "		<samples>"$num_parts"</samples>" >> $3
	echo "		<time>"`expr $aux_date - $init_date`"</time>" >> $3
	echo "	</profile>" >> $3 

	echo $num_parts" "`expr $aux_date - $init_date` >> benchmark_$3	
	
 #	aqui se elige la serie de simulaciones: lineal, exponencial o qué
        let num_parts=$num_parts*10
# 	let num_parts=$num_parts*2


done

echo "</results>" >> $3
