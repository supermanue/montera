'''
Created on Aug 30, 2012

@author: u5682
'''


from Application import Application
from DyTSS import DyTSS
from string import replace
from AppProfile import AppProfile
from base import Session
from Analyzer import Analyzer

#TODO: cambiar el bombre al metodo. Lee los requerimientose inicializa la aplicación y profiling
#TODO: de hecho, quizá sería mejor hacer esto en 2 pasos o algo
def readRequirements(requirementsFile):
	

	myRequirementsFile = open(requirementsFile,'r')
	
	myname = mydesiredSamples = mymaxSupportedSamples =  \
	mymaxProfilingTime = mypreProcessScript = mypostProcessScript = myinputFiles = myoutputFiles = myworkingDirectory = None
	myRemoteMontera = myLrms = myInfrastructure = None		
		
	for line in myRequirementsFile.readlines():
		normalizedLine = line.lower().strip()
		
		if normalizedLine.startswith("#"):
			continue;
		if normalizedLine.count("name") > 0:
			myname = line.split("=")[1].strip()
			
		elif normalizedLine.count("num_samples") > 0:
			mydesiredSamples= int(line.split("=")[1].strip())
		
		elif normalizedLine.count("postprocess_script") > 0:
			mypostProcessScript = line.split("=")[1].strip()

		elif normalizedLine.count("preprocessScript") > 0:
			mypreProcessScript = line.split("=")[1].strip()
			
		elif normalizedLine.count("max_profiling_time") > 0:
			mymaxProfilingTime = line.split("=")[1].strip()
			
		elif normalizedLine.count("max_supported_samples") > 0:
			mymaxSupportedSamples = line.split("=")[1].strip()
			
			#los ficheros de entrada estan separados por comas
		elif normalizedLine.count("input_files") > 0:
			myinputFiles = line.split("=")[1].strip().replace(' ', '')
			
		elif normalizedLine.count("output_files") > 0:
			myoutputFiles = line.split("=")[1].strip().replace(' ', '')

		elif normalizedLine.count("working_directory") > 0:
			myworkingDirectory = line.split("=")[1].strip()		
			
		elif normalizedLine.count("schedulingalgorithm") > 0:
			mySchedulingAlgorithm = normalizedLine.split("=")[1].strip()
		
		elif normalizedLine.count("remoteMontera") > 0:
			myRemoteMontera = normalizedLine.split("=")[1].strip()			
						
		elif normalizedLine.count("infrastructure") > 0:
			myInfrastructure = normalizedLine.split("=")[1].strip()	
	
		elif normalizedLine.count("parametricjob") > 0:
			print("Parametric Job");
			mySpecificationFile = line.split("=")[1].strip()
			myAnalyzer = Analyzer(mySpecificationFile)
			mydesiredSamples = myAnalyzer.numTasks
			myRemoteMontera = "parametricRemoteMontera.sh" 
			
			
	
	myApplicationProfile = Session.query(AppProfile).filter(AppProfile.applicationName == myname).first()
	if myApplicationProfile == None:
		myApplicationProfile = AppProfile(myname)
				
	myApplication = Application(myname, mydesiredSamples, 
			maxSupportedSamples = mymaxSupportedSamples,
			maxProfilingTime = mymaxProfilingTime,
			preProcessScript = mypreProcessScript,
			postProcessScript = mypostProcessScript,
			inputFiles = myinputFiles,
			outputFiles = myoutputFiles,
			workingDirectory = myworkingDirectory, 
			profile = myApplicationProfile,
			schedulingAlgorithm = mySchedulingAlgorithm,
			remoteMontera = myRemoteMontera,
			infrastructure = myInfrastructure)
	
	
	return myApplication

#TODO: acabar instrucciones
def printUsageInstructions():
	print("Usage:")
	print ("	Montera <application template> : will execute a new application as specified on the template")
	print ("	Montera recovery  : will recover last execution and continue with it")