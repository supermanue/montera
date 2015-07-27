'''
Created on Aug 24, 2012

@author: u5682
'''


from __future__ import division
import sys, os
import xml.dom.minidom
from xml.dom.minidom import Node
from DRMAA import *
import InformationManager
from AppProfile import AppProfile

from sqlalchemy import *
from base import Base, Session
import base
from sqlalchemy.orm import relationship, backref

from datetime import datetime

class Application(Base):
	'''
	classdocs
	'''
	__tablename__ = 'applications'
	id = Column(Integer, primary_key=True)
	name = Column(String)
	desiredSamples  = Column(Integer)
	remainingSamples = Column(Integer)
	maxSupportedSamples = Column(Integer)
	maxProfilingTime = Column(Integer)
	preProcessScript = Column(String)
	postProcessScript = Column(String)
	inputFiles = Column(String)
	outputFiles = Column(String)
	requirements = Column(String)
	environment  = Column(String)
	workingDirectory = Column(String)
	appProfileID = Column(Integer, ForeignKey('appProfiles.id'))
	finished = Column(Integer)
	schedulingAlgorithm=Column(String)
	remoteMontera=Column(String)


	profile = relationship("AppProfile", uselist=False)

	#parameters not in database
	size = -1


	def __init__(self, name, desiredSamples, 
				maxSupportedSamples = sys.maxint-1,
				maxProfilingTime = sys.maxint-1,
				preProcessScript = None,
				postProcessScript = None,
				inputFiles = None,
				outputFiles = None,
				requirements = None,
				environment = None,
				workingDirectory = None,
				profile = None,
				finished = 0,
				schedulingAlgorithm = "",
				remoteMontera = "maxSamples.sh", 
				infrastructure = None
				):
		'''
		Constructor
		'''
		
		
		self.name = name
		
		
		#sample stuff
		self.desiredSamples = desiredSamples
		self.remainingSamples = desiredSamples
		self.maxSupportedSamples=maxSupportedSamples

		self.maxProfilingTime = maxProfilingTime

		#profiling stuff
		#
		#ESTO ES UN POCO RARO
		#una aplicación tiene un profiling, un profiling tiene una aplicación
		#pero más de una aplicación puede tener un profiling
		#
		#por eso, cuando una aplicación en concreto se inicializa, tiene que decir al profiling
		#que es ella la que lo va a emplear en esa ocasión
		#eso es necesario si hay que hacer algún profiling de la aplicación, para poder coger
		#archivos de entrada, salida y demás
		
		self.profile = profile
		self.profile.application = self

		#fprocessing script
		self.preProcessScript = preProcessScript
		self.postProcessScript=postProcessScript
		
		#input and output files
		self.inputFiles=inputFiles
		self.outputFiles=outputFiles
		self.workingDirectory = workingDirectory
		self.size = -1
		self.size = self.calculateInputFileSize()
		
		
		#other stuff
		self.requirements=requirements
		self.environment=environment
		self.finished=finished
		self.schedulingAlgorithm = schedulingAlgorithm
		self.remoteMontera = remoteMontera
		self.infrastructure = infrastructure
		
	#NOTA: el status final de la tarea hay que ponerlo como  "CLEAR"
	def updateInfoAfterExecution(self, gridTask):
		
		print("Updating info after exetcution of task " +gridTask.gwID + " on host " + gridTask.host.hostname + " (hostID " + str(gridTask.host.id) + ")")
		gridTask.status="CLEAR"

		#1.- abrir el archivo correspondiente a esa task
		hostToUpdate = gridTask.host
				
		execution_file = base.tmpExecutionFolder + "/execution_result_" + gridTask.gwID + ".xml"
		
		#1.- abrir el archivo correspondiente a esa task
		try:
			doc = xml.dom.minidom.parse(execution_file)
		except:
			print("failed when updating info after execution. File " + execution_file + " could not be found")
			hostToUpdate.updateInfoAfterFailedExecution()
			Session.add(gridTask)
			return
		
		#si los archivos de salida deseados no existen, también la cuento como fallida
		for outputFile in self.outputFiles.split(","):
			
			#JOB_ID has to be replaced by gwID as it happens along the execution
			splittedFile = outputFile.split('JOB_ID')
			output=""
			for pos in range(len(splittedFile)):
				output += splittedFile[pos]
				if pos < len(splittedFile) -1:
					output+=gridTask.gwID
				
			
			if not os.path.exists(base.tmpExecutionFolder + "/" + output):
					print("failed when updating info after execution. output file " + base.tmpExecutionFolder + "/"  + output + " could not be found")
					hostToUpdate.updateInfoAfterFailedExecution()
					Session.add(gridTask)
					return	
		
		executionInfoList = doc.getElementsByTagName('execution_info')
		
		gridTaskType = None
		remoteHostName = None
		executionTime = None
		dataSize = None
		realSamples = None
		
		for executionData in executionInfoList:
			
			try:
				gridTaskType = executionData.getElementsByTagName("type")[0].firstChild.data #TODO: remove "unicode" from TEXT
				remoteHostName = executionData.getElementsByTagName("hostname")[0].firstChild.data
				executionTime = float(executionData.getElementsByTagName("execution_time")[0].firstChild.data)
				dataSize = float(executionData.getElementsByTagName("data_size")[0].firstChild.data)
				realSamples = int(executionData.getElementsByTagName("real_samples")[0].firstChild.data)
			except:
				print ("Error when reading execution file, exiting" )
				Session.add(gridTask)
				return

		#2.- procesar los resultados
			
		if gridTaskType != "execution":
			print ("ERROR when updating info from an application execution")
			print("Incorrect task type, expecting \"execution\"")
			gridTask.status = "CLEAR"
			Session.add(gridTask)
			return
			
		if remoteHostName != hostToUpdate.hostname:
			print ("ERROR when updating info from a application execution")
			print("Incorrect host name, expecting " + hostToUpdate.hostname)
			gridTask.status = "CLEAR"
			Session.add(gridTask)
			return
	
	
		if executionTime == 0:
			print ("ERROR when updating info from an application execution")
			print ("Execution time appears to be zero, and that's quite strange")
			gridTask.status = "CLEAR"
			hostToUpdate.updateInfoAfterFailedExecution()
			Session.add(gridTask)
			return 
		
		totalActiveTime = InformationManager.readTotalActiveTime(gridTask.gwID)
		if totalActiveTime == -1:
			print ("ERROR when updating info from an application execution")
			print ("Could not read active time from GridWay log, considering that task failed")
			gridTask.status = "CLEAR"
			hostToUpdate.updateInfoAfterFailedExecution()
			Session.add(gridTask)
			return 
		
		queueTime = InformationManager.readQueueTime(gridTask.gwID)
		if queueTime == -1:
			print ("ERROR when updating info from an application execution")
			print ("Could not read queue time from GridWay log, considering that task failed")
			gridTask.status = "CLEAR"
			hostToUpdate.updateInfoAfterFailedExecution()
			Session.add(gridTask)
			return 
		
		
		transferTime = totalActiveTime - executionTime
		if transferTime > 0:
			bandwidth = dataSize / transferTime
		else:
			bandwidth = -1
		#3.- actualizar rendimiento del host
		
		computationalEffort = self.profile.constantEffort + self.profile.sampleEffort * realSamples
		whetstones = computationalEffort / executionTime
		
		hostToUpdate.updateInfoFromSuccessFulExecution(whetstones, queueTime, bandwidth)
		hostToUpdate.failedProfilings -=1
		
		#4 actualizar estado de la tarea y la apliación.
		gridTask.realSamples = realSamples
		gridTask.status = "CLEAR"
		self.remainingSamples -= realSamples
		
		print("APPLICATION UPDATE: " + str(self.remainingSamples) + "/" + str(self.desiredSamples) + " left")		
	
		#5.- eliminar archivos temporales
		try:
			#os.remove(execution_file)
			print ("In Application.py, I would be deletign" + execution_file)
			print ("Execution file has been successfully deleted: " + execution_file)
		except:
			print ("Could not delete profiling file " + execution_file)

		#6 update info on DB
		gridTask.endDate = datetime.now()

		Session.add(gridTask)
		Session.add(self)
		
	
	def updateInfoAfterProfiling(self, gridTask):
		
		#1.- leer la info  correspondiente a esa aplicacion
		#tener en cuenta que es una serie de parejas de valores
		execution_file = base.tmpExecutionFolder + "execution_result_" + gridTask.gwID + ".xml"
		try:
			doc = xml.dom.minidom.parse(execution_file)
		except:
			print("failed when profiling host " + gridTask.host.hostname + ". File " + execution_file + " could not be found")
			gridTask.host.updateInfoAfterFailedProfiling()
			Session.add(gridTask.host)
			return
	
		executionInfoList = doc.getElementsByTagName('results')
		
		executionResults=[]
		for executionData in executionInfoList:
			profileInfoList = executionData.getElementsByTagName('profile')
			for profileInfo in profileInfoList: 
				samples = int(profileInfo.getElementsByTagName("samples")[0].firstChild.data) #TODO: remove "unicode" from TEXT
				time = int(profileInfo.getElementsByTagName("time")[0].firstChild.data)
				
				if samples == 0:
					print ("There was an error on host " + gridTask.host.hostname + ". File " + execution_file + ", no samples have been executed")
					gridTask.host.updateInfoAfterFailedProfiling()
					Session.add(gridTask.host)
					return 
				executionResults.append([samples, time])
		
		
		#2.- procesar los resultados
		#TODO: esto es muy posible que esté mal
		numSimulations = len(executionResults)
		if numSimulations == 0:
			print ("There was an error on host " + gridTask.host.hostname + ". File " + execution_file + ", no simulations performed")
			gridTask.host.updateInfoAfterFailedProfiling()
			Session.add(gridTask.host)
			return 
		avgSampleTime = 0
		for i in range(numSimulations-1):
			newSamples = executionResults[i+1][0] - executionResults[i][0]
			newTime = executionResults[i+1][1] - executionResults[i][1]

			if newTime <=0:
				value = 0 
				continue
			else:
				value = newTime / newSamples ######TODO esta es la clave!!! 
			
			#weighted sample time, so later executions are more important than the first ones
			#the reason is that they executed more samples, so their information is more valuable
			if i==0 or avgSampleTime == 0:
				avgSampleTime = value
			else:
				avgSampleTime = 0.6 * value + 0.4 * avgSampleTime
		
			
		acumConstantTime = 0

		#this has been modifiied to avoid zero values
		for i in range(len(executionResults)):
			acumConstantTime += max(0, executionResults[i][1] - executionResults[i][0] *  avgSampleTime) 
		avgConstantTime = acumConstantTime / numSimulations
		
		
		normalizedSampleEffort = avgSampleTime * gridTask.host.getWhetstones() 
		normalizedConstantEffort = avgConstantTime * gridTask.host.getWhetstones() 
		
		
		print("RESULTADO DEL PROFILING")
		print ("task id: " + gridTask.gwID)
		print("normalizado, this_sample_effort = " + str(normalizedSampleEffort))
		print("normalizado, this_constant_effort = " + str(normalizedConstantEffort))

		print("STATUS DEL PROFILING")
		print("normalizado, sample_effort = " + str(self.profile.sampleEffort))
		print("normalizado, constant_effort = " + str(self.profile.constantEffort))
		print("")


		if (normalizedSampleEffort < 0 ) or (normalizedConstantEffort < 0):
			print ("A normalized effort below zero makes no sense, dismissing profile")
			return
		self.profile.updateInfoAfterProfiling(normalizedConstantEffort, normalizedSampleEffort)
	
		Session.add(self.profile)
	
	
	
	#the idea behind this method is that in only consults the file size once each execution. Then, variable self.inputFileSize
	#acts like a cache, thus speeding up the process	
	def calculateInputFileSize(self):
		
		if self.size >=0:
			return self.size
		
		if self.workingDirectory == None:
			filePath = "."
		else:
			filePath = self.workingDirectory
		
		if self.inputFiles == None:
			print ("No input files")
			self.size = 0
			return
		
		totalFilesize = 0	
		for inputFile in self.inputFiles.split(","):
			completeFileName = filePath + "/" + inputFile
			if not os.path.exists(completeFileName):
				print("FATAL ERROR: input file does not exist")
				print("file name: " + completeFileName)
				print("exiting...")
				sys.exit()
			fileSize= os.path.getsize(completeFileName)
			totalFilesize +=fileSize
		
		#KB to MB
		totalFilesize = totalFilesize / 1024
		
		self.size = totalFilesize
		return totalFilesize
			
			
			
	def exportInfoToFile(self, path):
		result=""
		result+="<name>" + self.name + "</name>\n"
		result+="<constant_effort>" + str(self.profile.constantEffort) + "</constant_effort>\n"
		result+="<sample_effort>" + str(self.profile.sampleEffort) + "</sample_effort>\n"
		
		f = open(path, 'w')
		f.write(result)
		f.close()
