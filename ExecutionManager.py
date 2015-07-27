'''
Created on Aug 29, 2012

@author: u5682
'''
from DRMAA import *
import os, sys, shutil
from datetime import datetime
from time import sleep
import socket

from base import Session, profilingFiles
import base
from GridTask import GridTask
from Pilot import Pilot

class ExecutionManager(object):
	'''
	classdocs
	'''	

def initGridSession():

	retryCounter=0
	while retryCounter < base.initRetryNumber:
		
		(result, error)=drmaa_init(None)
		if result == DRMAA_ERRNO_ALREADY_ACTIVE_SESSION:
			print "drmaa_init(): session was already intialized"
			return
		elif result != DRMAA_ERRNO_SUCCESS :
			print "drmaa_init() failed: "+error
			print ("Retrying.... " + str(retryCounter) + "/" + str(base.initRetryNumber))
			sleep(3)
			retryCounter += 1
			
		else:
			print "drmaa_init() success"
			return
	
	print ("Impossible to stablish contact with GridWay, exiting...")	
	sys.exit(-1)

def exitGridSession():
	(result, error)=drmaa_exit()
	if result == DRMAA_ERRNO_NO_ACTIVE_SESSION:
		print "drmaa_exit(): there was no active session "

	elif result != DRMAA_ERRNO_SUCCESS:
		print "drmaa_exit() failed: "+error
		sys.exit(-1)
	else:
		print "drmaa_exit() success"

def submit(gridTask):
	print ("Submitting new task")
	print ("	type: " + gridTask.type)
	print ("	host: " + gridTask.host.hostname)
	print ("	Host type: " + str(gridTask.host.__class__))

	if gridTask.type == "applicationExecution":
		print ("	samples: " + str(gridTask.maxSamples))
	print ("")

	retryCounter=0
	while retryCounter < base.initRetryNumber:
		
		(result, job_id, error)=drmaa_run_job(gridTask.template)
		if result==DRMAA_ERRNO_SUCCESS:
			print ("Job successfully submited ID: %s" % (job_id))
                        gridTask.gwID = job_id
                        return


		elif result == DRMAA_ERRNO_INTERNAL_ERROR:
			print ("drmaa_run_job() failed: %s" % (error))
			print ("DRMAA_ERRNO_INTERNAL_ERROR")
			print ("Exiting session")
			exitGridSession()
			print ("Aboritng Montera")
			sys.exit(-1)

                elif result == DRMAA_ERRNO_DRM_COMMUNICATION_FAILURE:
                        print ("drmaa_run_job() failed: %s" % (error))
                        print ("DRMAA_ERRNO_DRM_COMMUNICATION_FAILURE")
                        print ("Exiting session")
                        exitGridSession()
			sleep (5)
			print ("")	
			print ("Retrying.... " + str(retryCounter) + "/" + str(base.initRetryNumber))
			print ("Opening session")
			initGridSession()
			retryCounter += 1

                elif result == DRMAA_ERRNO_TRY_LATER:
                        print ("drmaa_run_job() failed: %s" % (error))
                        print ("DRMAA_ERRNO_TRY_LATER")
                        print ("Exiting session")
                        exitGridSession()
                        sleep (15)
                        print ("")      
                        print ("Retrying.... " + str(retryCounter) + "/" + str(base.initRetryNumber))
                        print ("Opening session")
                        initGridSession()
                        retryCounter += 1

                elif result == DRMAA_ERRNO_NO_ACTIVE_SESSION:
                        print ("drmaa_run_job() failed: %s" % (error))
                        print ("DRMAA_ERRNO_NO_ACTIVE_SESSION")
                        print ("Retrying.... " + str(retryCounter) + "/" + str(base.initRetryNumber))
                        print ("Opening session")
                        initGridSession()
                        retryCounter += 1

			
	print ("Could not submit task, aborting execution")	
	sys.exit(-1)
	
	

#WAITING TIME se mide en segundos
def waitForTermination(gridTasksToSynchronize, waitingTime=DRMAA_TIMEOUT_WAIT_FOREVER):
	#synchronize tasks
	tasksToSynchronize = []
	
	for gridTask in gridTasksToSynchronize:
		if gridTask.gwID == "-1":
			print("task " + gridTask.id + " has not been submitted to the Grid, will not wait for it")
			continue
		tasksToSynchronize.append(gridTask.gwID)
		
	#(result, error)=drmaa_synchronize(tasksToSynchronize, waitingTime, 0)
	print ("Removed syncrhonization to avoid system crash. Sleeping for " + str(waitingTime) + " instead")
	sleep(waitingTime)
	result=0

	#con esto se cancela toda la ejecución
	#===========================================================================
	# if result != DRMAA_ERRNO_SUCCESS:
	#	print >> sys.stderr, "drmaa_synchronize failed: %s" % (error)
	#	sys.exit(-1)
	#===========================================================================
	if result != DRMAA_ERRNO_SUCCESS:
		print ("Not all tasks could be synchronized: %s" % (error))


	
	# posibles estados de un trabajo con GW + DRMAA:
	#	 - DRMAA_PS_UNDETERMINED: An UNDETERMINED state can either obtained due
	#  to a  communication error with the GridWay daemon, or because the job has
	#  not been initialized yet.
	#
	#	  - DRMAA_PS_QUEUED_ACTIVE The job has been successfully submitted and 
	#  it is pending to be scheduled.
	#
	#	  - DRMAA_PS_RUNNING The job has been successfully submitted to a 
	# remote host. Please note that once submitted, the job can be in any of the 
	# execution stages, namely: prolog (file stage-in), wrapper (execution), 
	# epilog (file stage-out) or migrating (to another host).
	#
	#	  - DRMAA_PS_USER_ON_HOLD The job has been held by the user
	#
	#	  - DRMAA_PS_DONE Job has been completely executed and output files are 
	#  available at the client. drmaa_wait() and drmaa_synchronize() calls on the 
	#  job will return immediately. Also rusage information is available.  
	#
	#	  - DRMAA_PS_FAILED Job execution has failed, and the "on_failure" policy
	#  is to hold it on FAILED state.
	
	
	#TODO: pensar bien donde y por que se llama a este metodo
def updateGridTaskStatus(gridTask):
	#get GridWay status
	(result, status, error)=drmaa_job_ps(gridTask.gwID)

#error control
	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "updatingGridTaskStatus, drmaa_job_ps() failed for task " + gridTask.gwID + ". Error: %s" % (error)
		gridTask.status = "DONE" #DONE does not imply success, just that it has finished
	else:
		if gridTask.status == "CLEAR": #ya se ha acabado, nothing to do here
			return
		
		elif status == DRMAA_PS_UNDETERMINED or status == DRMAA_PS_QUEUED_ACTIVE:
			gridTask.status = "SUBMITTED"
			
		elif status == DRMAA_PS_RUNNING:
			#if needed, update task execution start
			if gridTask.status == "SUBMITTED" or gridTask.status=="WAITING":
				gridTask.executionStartDate = datetime.now()
				print ("Task " + gridTask.gwID + "with type " + gridTask.type + "  on host " + gridTask.host.hostname +  " with ID=" + str(gridTask.host.id) + " has started its execution")
			gridTask.status = "RUNNING"
		
		elif status == DRMAA_PS_DONE or status == DRMAA_PS_FAILED:
			gridTask.status = "DONE"
	
	Session.add(gridTask)
	
#===============================================================================
#	
#	
# def createGridTask(application, host, minNumSamples, maxNumSamples):
#	
#	print ("TASK created")
#	#create job template
#	(result, jt, error)=drmaa_allocate_job_template()
# 
#	if result != DRMAA_ERRNO_SUCCESS:
#		print >> sys.stderr, "drmaa_allocate_job_template() failed: %s" % (error)
#		sys.exit(-1)
# 
#	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, application.name + "_execution.jt")
# 
#	workingDirectory = base.tmpExecutionFolder
#	(result, error)=drmaa_set_attribute(jt, DRMAA_WD, workingDirectory)
# 
#	
#	if result != DRMAA_ERRNO_SUCCESS:
#		print >> sys.stderr, "Error setting job template attribute: %s" % (error)
#		sys.exit(-1)
# 
#	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, application.name + "_template")
#	(result, error)=drmaa_set_attribute(jt, DRMAA_REMOTE_COMMAND, "/bin/sh")
#	
#		
#	arguments = []
#	arguments.append("remoteMontera.sh")
#	arguments.append(application.name)
#	arguments.append(DRMAA_GW_JOB_ID)	
#	arguments.append(str(minNumSamples))	
#	arguments.append(str(maxNumSamples))	
#	arguments.append(application.remoteMontera)
# 
# 
#	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_ARGV, arguments)#TODO: esto quiza esta mal, deberia ser un array de strings
# 
#	if result != DRMAA_ERRNO_SUCCESS:
#		print >> sys.stderr,"Error setting remote command arguments: %s" % (error)
#		sys.exit(-1)
# 
#	(result, error)=drmaa_set_attribute(jt, DRMAA_OUTPUT_PATH, "stdout." + DRMAA_GW_JOB_ID) #TODO: en el howto4 aqui ponen 2 puntos (:stdout...). Tenerlo en cuenta si no funciona
#	(result, error)=drmaa_set_attribute(jt, DRMAA_ERROR_PATH,"stderr." + DRMAA_GW_JOB_ID)
# 
# 
#	
# 
#	inputFiles = []
#	
#	inputFiles.append("file://" + base.executionFiles + "remoteMontera.sh")
#	inputFiles.append("file://" + base.executionFiles + "maxSamples.sh")
#	
#	#host information file
#	host.exportInfoToFile(workingDirectory+ host.hostname + "_profile")
#	inputFiles.append("file://" + workingDirectory + host.hostname + "_profile"+ " resource_info.tmp")
#	
#	#app information file
#	application.exportInfoToFile(workingDirectory + application.name + "_profiling.tmp")
#	inputFiles.append("file://" + workingDirectory + application.name + "_profiling.tmp")
#	
#	
#	#application-specific files
#	for inputF in application.inputFiles.split(','): #copiamos los ficheros al directorio de trabajo
#		#=======================================================================
#		# inputFiles.append(inputF)
#		#=======================================================================
#		inputFiles.append("file://" +application.workingDirectory + "/" + inputF)
#		
#	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_INPUT_FILES, inputFiles)
#	
#	
#	
#	outputFiles = []
#	for outputF in application.outputFiles.split(','):
#		splittedFile = outputF.split('JOB_ID')
#		output=""
#		for pos in range(len(splittedFile)):
#			output += splittedFile[pos]
#			if pos < len(splittedFile) -1:
#				output+=DRMAA_GW_JOB_ID
#		outputFiles.append(output)	
#	
#	outputFiles.append("execution_result_"+DRMAA_GW_JOB_ID+".xml")
#	outputFiles.append(application.name + "_" + DRMAA_GW_JOB_ID+ "_output.txt")
# 
#		
#	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_OUTPUT_FILES, outputFiles)
# 
#	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_REQUIREMENTS,"HOSTNAME="+'"'+host.hostname+'"')
# 
#	#create Grid task
#	myGridTask = GridTask(host, jt, "applicationExecution", minSamples = minNumSamples, maxSamples = maxNumSamples, application = application)
#		
#	return myGridTask
#===============================================================================


def createGWTemplate(gridTask):
	
	application = gridTask.application
	maxSuspensionTime = 3600
	
	host = gridTask.host
	minNumSamples = gridTask.minSamples
	maxNumSamples = gridTask.maxSamples
	#create job template
	(result, jt, error)=drmaa_allocate_job_template()

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "drmaa_allocate_job_template() failed: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, application.name + "_execution.jt")


	workingDirectory = base.tmpExecutionFolder
	
	(result, error)=drmaa_set_attribute(jt, DRMAA_WD, workingDirectory)

	
	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "Error setting job template attribute: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, application.name + "_template")
	(result, error)=drmaa_set_attribute(jt, DRMAA_REMOTE_COMMAND, "/bin/sh")
	
		
	arguments = []
	arguments.append("remoteMontera.sh")
	arguments.append(application.name)
	arguments.append(DRMAA_GW_JOB_ID)	
	arguments.append(str(minNumSamples))	
	arguments.append(str(maxNumSamples))	
	arguments.append("maxSamples.sh")


	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_ARGV, arguments)#TODO: esto quiza esta mal, deberia ser un array de strings

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr,"Error setting remote command arguments: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_OUTPUT_PATH, "stdout." + DRMAA_GW_JOB_ID) #TODO: en el howto4 aqui ponen 2 puntos (:stdout...). Tenerlo en cuenta si no funciona
	(result, error)=drmaa_set_attribute(jt, DRMAA_ERROR_PATH,"stderr." + DRMAA_GW_JOB_ID)

	inputFiles = []

	inputFiles.append("file://" + base.executionFiles + "remoteMontera.sh")
	inputFiles.append("file://" + base.executionFiles + "maxSamples.sh")
	
	#host information file
	host.exportInfoToFile(workingDirectory + host.hostname + "_profile")
	inputFiles.append("file://" + workingDirectory +  host.hostname + "_profile"+ " resource_info.tmp")
	
	#app information file
	application.exportInfoToFile(workingDirectory + application.name + "_profiling.tmp")
	inputFiles.append("file://" + workingDirectory + application.name + "_profiling.tmp")
	
	
	#application-specific files
	for inputF in application.inputFiles.split(','): #copiamos los ficheros al directorio de trabajo
		inputFiles.append("file://" +application.workingDirectory + "/" + inputF)


	try:		
		(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_INPUT_FILES, inputFiles)
	except:
		print ("error when asigning input files, trying again. These are the files")
		print ("")
		print ("ExecutionManager: createGWTemplate")
		print("	input files:")
		inputFiles2 = []
		for fileName in inputFiles:
			print ("	" + fileName)
			inputFiles2.append(str(fileName))
		(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_INPUT_FILES, inputFiles2)
	
	
	outputFiles = []
	for outputF in application.outputFiles.split(','):
		splittedFile = outputF.split('JOB_ID')
		output=""
		for pos in range(len(splittedFile)):
			output += splittedFile[pos]
			if pos < len(splittedFile) -1:
				output+=DRMAA_GW_JOB_ID
		outputFiles.append(output)	
	
	outputFiles.append("execution_result_"+DRMAA_GW_JOB_ID+".xml")
	outputFiles.append(application.name + "_" + DRMAA_GW_JOB_ID+ "_output.txt")
		
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_OUTPUT_FILES, outputFiles)

	if host.hostname != "":
		(result, error)=drmaa_set_attribute(jt, DRMAA_GW_REQUIREMENTS,"HOSTNAME="+'"'+host.hostname+'"')

	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_SUSPENSION_TIMEOUT ,str(maxSuspensionTime))

	#create Grid task
	gridTask.template=jt
	gridTask.type = "applicationExecution"

	return gridTask


	
def createProfilingTask(host, application):
	maxSuspensionTime = 3600

	#create job template
	(result, jt, error)=drmaa_allocate_job_template()

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "drmaa_allocate_job_template() failed: %s" % (error)
		sys.exit(-1)

	workingDirectory = base.tmpExecutionFolder
	(result, error)=drmaa_set_attribute(jt, DRMAA_WD, workingDirectory)
	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "Error setting job template attribute: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, "Profile.jt")
	(result, error)=drmaa_set_attribute(jt, DRMAA_REMOTE_COMMAND, "/bin/sh")
	
		
	arguments = []
	arguments.append("profile.sh")
	arguments.append(application.profile.applicationName)	
	arguments.append(str(application.maxProfilingTime))	
	arguments.append("execution_result_"+DRMAA_GW_JOB_ID+".xml")

	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_ARGV, arguments)#

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr,"Error setting remote command arguments: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_OUTPUT_PATH, "stdout." + DRMAA_GW_JOB_ID)
	(result, error)=drmaa_set_attribute(jt, DRMAA_ERROR_PATH,"stderr." + DRMAA_GW_JOB_ID)

	inputFiles = []
	inputFiles.append("file://" + profilingFiles + "profile.sh")
	inputFiles.append("file://" + profilingFiles+ "whetstone")
	
	for inputF in application.inputFiles.split(','): #copiamos los ficheros al directorio de trabajo
		inputFiles.append("file://" + application.workingDirectory + "/" + inputF)
	
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_INPUT_FILES, inputFiles)
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_OUTPUT_FILES, ["execution_result_"+DRMAA_GW_JOB_ID+".xml", "applicationOutput_"+DRMAA_GW_JOB_ID+".txt"])

	if host.hostname != "":
		(result, error)=drmaa_set_attribute(jt, DRMAA_GW_REQUIREMENTS,"HOSTNAME="+'"'+host.hostname+'"')

	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_SUSPENSION_TIMEOUT ,str(maxSuspensionTime))

	#create Grid task
	myGridTask = GridTask(host, jt, "applicationProfiling",application = application)
	
	return myGridTask


def createHostProfilingTask(host):
	
	#create job template
	(result, jt, error)=drmaa_allocate_job_template()

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "drmaa_allocate_job_template() failed: %s" % (error)
		sys.exit(-1)

	workingDirectory = base.tmpExecutionFolder
	
	(result, error)=drmaa_set_attribute(jt, DRMAA_WD, workingDirectory)
	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "Error setting job template attribute: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, "Benchmarking.jt")
	(result, error)=drmaa_set_attribute(jt, DRMAA_REMOTE_COMMAND, "/bin/sh")
	
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_ARGV, ["benchmark.sh"])#TODO: esto quiza esta mal, deberia ser un array de strings

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr,"Error setting remote command arguments: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_OUTPUT_PATH, "stdout." + DRMAA_GW_JOB_ID) #TODO: en el howto4 aqui ponen 2 puntos (:stdout...). Tenerlo en cuenta si no funciona
	(result, error)=drmaa_set_attribute(jt, DRMAA_ERROR_PATH,"stderr." + DRMAA_GW_JOB_ID)

	inputFiles = []
	inputFiles.append("file://" + base.profilingFiles + "benchmark.sh")
	inputFiles.append("file://" + base.profilingFiles+ "whetstone")
	inputFiles.append("file://" + base.profilingFiles + "empty_file")
	
	
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_INPUT_FILES, inputFiles)
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_OUTPUT_FILES, ["execution_result_"+DRMAA_GW_JOB_ID+".xml"])

	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_REQUIREMENTS,"HOSTNAME="+'"'+host.hostname+'"')
	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_SUSPENSION_TIMEOUT,"36000")
	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_RESCHEDULE_ON_FAILURE,"no")

	#create Grid task
	
	myGridTask = GridTask(host, jt, "hostProfiling")
	
	return myGridTask




def createPilotTask(host):
	
	#create job template
	(result, jt, error)=drmaa_allocate_job_template()

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "drmaa_allocate_job_template() failed: %s" % (error)
		sys.exit(-1)

	workingDirectory = base.tmpExecutionFolder
	
	(result, error)=drmaa_set_attribute(jt, DRMAA_WD, workingDirectory)
	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "Error setting job template attribute: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, "pilot.jt")
	(result, error)=drmaa_set_attribute(jt, DRMAA_REMOTE_COMMAND, "/usr/bin/python")
	
#	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_ARGV, ["pilot.py","-s","gw02.ciemat.es","-p","24996","-f","15","-t","1","-i","45","--nosec"])#TODO: esto quiza esta mal, deberia ser un array de strings

	hostname = socket.gethostname()
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_ARGV, ["pilot.py","-s", hostname,"-p","24996","-f","20","-t","3","-i","30","--nosec"])#TODO: esto quiza esta mal, deberia ser un array de strings

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr,"Error setting remote command arguments: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_OUTPUT_PATH, "pilot.out." + DRMAA_GW_JOB_ID) #TODO: en el howto4 aqui ponen 2 puntos (:stdout...). Tenerlo en cuenta si no funciona
	(result, error)=drmaa_set_attribute(jt, DRMAA_ERROR_PATH,"pilot.err." + DRMAA_GW_JOB_ID)

	inputFiles = []
	inputFiles.append("file://" + base.pilotFiles + "pilot.py")
	inputFiles.append("file://" + base.pilotFiles+ "syscommand.py")
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_INPUT_FILES, inputFiles)
	
	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_REQUIREMENTS,"(!(LRMS_NAME=\"jobmanager-pilot\")) & HOSTNAME="+'"'+host.hostname+'"')
	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_RESCHEDULE_ON_FAILURE,"no")
	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_SUSPENSION_TIMEOUT,"3600")


	#create Grid task
	
	myGridTask = GridTask(host, jt, "pilot")
	
	return myGridTask



def createNewGWTemplate(gridTask):
	
	application = gridTask.application
	maxSuspensionTime = 3600
	
	host = gridTask.host
	minNumSamples = gridTask.minSamples
	maxNumSamples = gridTask.maxSamples
	#create job template
	(result, jt, error)=drmaa_allocate_job_template()

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "drmaa_allocate_job_template() failed: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, application.name + "_execution.jt")


	workingDirectory = base.tmpExecutionFolder
	
	(result, error)=drmaa_set_attribute(jt, DRMAA_WD, workingDirectory)

	
	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "Error setting job template attribute: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, application.name + "_template")
	(result, error)=drmaa_set_attribute(jt, DRMAA_REMOTE_COMMAND, "/bin/sh")
	
		
	arguments = []
	arguments.append("remoteMontera.sh")
	arguments.append(application.name)
	arguments.append(DRMAA_GW_JOB_ID)	
	arguments.append(str(minNumSamples))	
	arguments.append(str(maxNumSamples))	
	arguments.append("maxSamples.sh")


	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_ARGV, arguments)#TODO: esto quiza esta mal, deberia ser un array de strings

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr,"Error setting remote command arguments: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_OUTPUT_PATH, "stdout." + DRMAA_GW_JOB_ID) #TODO: en el howto4 aqui ponen 2 puntos (:stdout...). Tenerlo en cuenta si no funciona
	(result, error)=drmaa_set_attribute(jt, DRMAA_ERROR_PATH,"stderr." + DRMAA_GW_JOB_ID)

	inputFiles = []

	inputFiles.append("file://" + base.executionFiles + "remoteMontera.sh")
	inputFiles.append("file://" + base.executionFiles + "maxSamples.sh")
	inputFiles.append("file://" + base.profilingFiles+ "whetstone")
	
	#host information file
	host.exportInfoToFile(workingDirectory + host.hostname + "_profile")
	inputFiles.append("file://" + workingDirectory +  host.hostname + "_profile"+ " resource_info.tmp")
	
	#app information file
	application.exportInfoToFile(workingDirectory + application.name + "_profiling.tmp")
	inputFiles.append("file://" + workingDirectory + application.name + "_profiling.tmp")
	
	
	#application-specific files
	for inputF in application.inputFiles.split(','): #copiamos los ficheros al directorio de trabajo
		inputFiles.append("file://" +application.workingDirectory + "/" + inputF)


	try:		
		(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_INPUT_FILES, inputFiles)
	except:
		print ("error when asigning input files, trying again. These are the files")
		print ("")
		print ("ExecutionManager: createGWTemplate")
		print("	input files:")
		inputFiles2 = []
		for fileName in inputFiles:
			print ("	" + fileName)
			inputFiles2.append(str(fileName))
		(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_INPUT_FILES, inputFiles2)
	
	
	outputFiles = []
	for outputF in application.outputFiles.split(','):
		splittedFile = outputF.split('JOB_ID')
		output=""
		for pos in range(len(splittedFile)):
			output += splittedFile[pos]
			if pos < len(splittedFile) -1:
				output+=DRMAA_GW_JOB_ID
		outputFiles.append(output)	
	
	outputFiles.append("execution_result_"+DRMAA_GW_JOB_ID+".xml")
	outputFiles.append(application.name + "_" + DRMAA_GW_JOB_ID+ "_output.txt")
		
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_OUTPUT_FILES, outputFiles)

	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_REQUIREMENTS,"HOSTNAME="+'"'+host.hostname+'"')

	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_SUSPENSION_TIMEOUT ,str(maxSuspensionTime))

	#create Grid task
	gridTask.template=jt
	gridTask.type = "applicationExecution"

	return gridTask

		

def removeTaskFromGW(gridTask):
		print ("removing task " + gridTask.gwID + " from GridWay")
		gwID = gridTask.gwID
		(result, job_id_out, stat, rusage, error)=drmaa_wait(gwID, DRMAA_TIMEOUT_NO_WAIT)
		if result == DRMAA_ERRNO_SUCCESS:
			print ("	Done")
			return
		
		(result, status, error)=drmaa_job_ps(gridTask.gwID)
		if status == DRMAA_PS_UNDETERMINED:
			(result, error)=drmaa_control(gwID, DRMAA_CONTROL_TERMINATE)
			if result == DRMAA_ERRNO_SUCCESS:
				print ("	Done")
				return
		print ("	Task could  not be removed from GridWay by Montera, the system will take care of that")
			
			
			#this is the perfect solution, but it hangs if a task cannot be removed
			
	#===========================================================================
	#		
	#		(result, error)=drmaa_control(gwID, DRMAA_CONTROL_TERMINATE)
	#		if result == DRMAA_ERRNO_SUCCESS:
	#			print ("	Done")
	#		
	#		if result != DRMAA_ERRNO_SUCCESS:
	#			print("ERROR, when deleting task. Maybe not present on system")
	# 
#===========================================================================


def cleanTmpDirectory():
	if not os.path.exists(base.tmpExecutionFolder):
		print ("Nothing to be cleaned")
		return
	
	for fileName in os.listdir(base.tmpExecutionFolder):
		longName = base.tmpExecutionFolder + "/" + fileName
		if not os.path.isdir(longName):
			#os.remove(longName)
			print ("in ExecutionManager.cleanTmpDirectory, I would be deleting" + longName)
			
			

#metodo utilizado por la aproximación 3 para envair trabajos a los disintos hosts, levantar los pilots

def createFakeHostProfilingTask(host):
	
	#create job template
	(result, jt, error)=drmaa_allocate_job_template()

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "drmaa_allocate_job_template() failed: %s" % (error)
		sys.exit(-1)

	workingDirectory = base.tmpExecutionFolder
	
	(result, error)=drmaa_set_attribute(jt, DRMAA_WD, workingDirectory)
	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "Error setting job template attribute: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, "FakehostProfiling.jt")
	(result, error)=drmaa_set_attribute(jt, DRMAA_REMOTE_COMMAND, "/bin/sh")
	
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_ARGV, ["benchmark.sh"])#TODO: esto quiza esta mal, deberia ser un array de strings

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr,"Error setting remote command arguments: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_OUTPUT_PATH, "stdout." + DRMAA_GW_JOB_ID) #TODO: en el howto4 aqui ponen 2 puntos (:stdout...). Tenerlo en cuenta si no funciona
	(result, error)=drmaa_set_attribute(jt, DRMAA_ERROR_PATH,"stderr." + DRMAA_GW_JOB_ID)

	inputFiles = []
	inputFiles.append("file://" + base.profilingFiles + "benchmark.sh")
	inputFiles.append("file://" + base.profilingFiles+ "whetstone")
	inputFiles.append("file://" + base.profilingFiles + "empty_file")
	
	
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_INPUT_FILES, inputFiles)
	(result, error)=drmaa_set_vector_attribute(jt, DRMAA_V_GW_OUTPUT_FILES, ["execution_result_"+DRMAA_GW_JOB_ID+".xml"])

	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_REQUIREMENTS,'(LRMS_NAME="jobmanager-pilot") & (PILOT_u5682_VAR_5>65534)')
#	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_REQUIREMENTS,'(LRMS_NAME="jobmanager-pilot")')

	#create Grid task
	
	myGridTask = GridTask(host, jt, "hostProfiling")
	
	return myGridTask


#task created to start a pilot
def createWakeUpask(host):
	
	#create job template
	(result, jt, error)=drmaa_allocate_job_template()

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "drmaa_allocate_job_template() failed: %s" % (error)
		sys.exit(-1)

	workingDirectory = base.tmpExecutionFolder
	
	(result, error)=drmaa_set_attribute(jt, DRMAA_WD, workingDirectory)
	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr, "Error setting job template attribute: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_JOB_NAME, "WakeUp.jt")
	(result, error)=drmaa_set_attribute(jt, DRMAA_REMOTE_COMMAND, "/bin/uname")
	

	if result != DRMAA_ERRNO_SUCCESS:
		print >> sys.stderr,"Error setting remote command arguments: %s" % (error)
		sys.exit(-1)

	(result, error)=drmaa_set_attribute(jt, DRMAA_OUTPUT_PATH, "stdout." + DRMAA_GW_JOB_ID) #TODO: en el howto4 aqui ponen 2 puntos (:stdout...). Tenerlo en cuenta si no funciona
	(result, error)=drmaa_set_attribute(jt, DRMAA_ERROR_PATH,"stderr." + DRMAA_GW_JOB_ID)

	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_REQUIREMENTS,'(LRMS_NAME="jobmanager-pilot") & (PILOT_u5682_VAR_5>65534)')
#	(result, error)=drmaa_set_attribute(jt, DRMAA_GW_REQUIREMENTS,'(LRMS_NAME="jobmanager-pilot")')

	#create Grid task
	
	myGridTask = GridTask(host, jt, "wakeUp")
	
	return myGridTask
