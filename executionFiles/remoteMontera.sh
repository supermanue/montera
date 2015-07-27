#!/bin/sh

###########
## TO DO
###########
#Exportar todas las variables de este host del fichero XML


#PARAMS OF MONTERA REMOTO
#$1: name of the app (or script) to execute.
export name=$1
#$2: id of the chunk
export chunk_id=$2
#$3: min samples
export min_samples=$3
#$4: max samples
export max_samples=$4
#$5: chosen politic. The script corresponding to the chosen politic must be copied to the remote site too!
politic=$5


#aux function to extract a value from a XML file
#param 1: field to stract
#(format must be something like<field>value</field> )
parse()
{
	file=$1
	xml_param=$2
	val=`more $file | grep "<$xml_param>.*</$xml_param>" | sed -e "s/^.*<$xml_param/<$xml_param/" | cut -f2 -d">"| cut -f1 -d"<"`
	echo $val
}

export constant_effort=`parse "$name"_profiling.tmp constant_effort`
export sample_effort=`parse "$name"_profiling.tmp sample_effort`

#hostname
hostname=$GW_HOSTNAME

#info in XML file is something like
#<hostname>hydrus.dacya.ucm.es</hostname>
#<arch>null</arch>
#<cpu_mhz>0</cpu_mhz>
#<max_cpu_time>2880</max_cpu_time>
#<whetstones>-1.0</whetstones>

#store the host info in a temp file
#DEBIG grep -i9 $hostname resource_info.tmp | tail -n 10 > resource_info.tmp

export whetstones=`parse resource_info.tmp whetstones`
export average_queue_time=`parse resource_info.tmp queue_time`



#We store the size of the folder in order to calculate bandwith
data_size=`du -s | awk '{print $1}'`

#this is a timestamp to control the execution time
init_time=`date +%s`

#now, execute the chosen politic
#and store the number of samples simulated
echo "EN REMOTE_MONTERA"
echo "politics vale" $politic
echo "constant_effort vale" $constant_effort
echo "sample_effort vale" $sample_effort
echo "whetstones vale" $whetstones
echo "y la hora es: " `date`

case $politic in 
	"maxSamples.sh") 
		real_samples=`sh -x maxSamples.sh` ;;
	"deadline.sh") 
		real_samples=`sh -x deadline.sh` ;;
	"avg_queue_samples.sh")
	#ESTA POLITICA HAY QUE REFINARLA
		real_samples=`sh -x avg_queue_samples.sh $name $chunk_id $whetstones` ;;
	"parametricRemoteMontera.sh")
		real_samples=`sh -x maxSamples.sh` ;;
	*) echo "no politic chosen; exiting...";;
esac

end_time=`date +%s`
execution_time=`expr $end_time - $init_time` 

#we generate an XML file to store the info info of the execution, so MonteraLocal is aware of what
# happened
output_file="execution_result_"$GW_JOB_ID".xml"

echo "<execution_info>" > $output_file
echo "	<type>execution</type>" >> $output_file
echo "	<hostname>"$hostname"</hostname>" >> $output_file 
echo "	<whetstones>"$whetstones"</whetstones>" >> $output_file
echo "	<real_samples>"$real_samples"</real_samples>" >> $output_file
echo "	<execution_time>"$execution_time"</execution_time>" >> $output_file
echo "	<data_size>"$data_size"</data_size>" >> $output_file
echo "	<constant_effort>"$constant_effort"</constant_effort>" >>$output_file
echo "	<sample_effort>"$sample_effort"</sample_effort>" >> $output_file
echo "</execution_info>" >> $output_file


echo "AQUI HAY"
ls -plah

execution_whetstones=`echo "( $constant_effort + $real_samples * $sample_effort ) / $execution_time " | bc`
if [ -n "$PILOT_u5682_VAR_5" ]; then
	echo "float: " $execution_whetstones 
	whetstonesInt=${execution_whetstones%.*}
	echo $whetstonesInt > $PILOT_u5682_VAR_5
fi 

