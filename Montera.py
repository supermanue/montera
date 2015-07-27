'''
Created on Aug 31, 2012

@author: u5682
'''
from sqlalchemy import *

import base

from datetime import datetime

from DBDesign import DBDesign
from Host import Host
from Controller import Controller
import UserInterface
import ExecutionManager
import ExecutionStats
import Infrastructure, PilotInfrastructure, NewPilotInfrastructure
import RefreshCertificates

import shutil

import sys
sys.settrace

import os
os.environ['LD_LIBRARY_PATH'] = "/soft/users/home/u5682/nfs/todo_GATE/CLHEP/lib:/soft/users/home/u5682/nfs/todo_GATE/root/lib:/opt/gw/lib:/opt/d-cache/dcap/lib:/opt/d-cache/dcap/lib64:/opt/glite/lib:/opt/glite/lib64:/opt/globus/lib:/opt/lcg/lib:/opt/lcg/lib64:/opt/classads/lib64/:/opt/c-ares/lib"


if __name__ == '__main__':
	print("WELLCOME TO MONTERA 2.0")
	print("---")
	print ("date:" + str(datetime.now()))
	
	print("")
	if len(sys.argv) != 2:
		UserInterface.printUsageInstructions()
		print("Exiting...")
		sys.exit(1)
		
	requirementsFile=sys.argv[1]
	

	
	print("Starting connection with database")
			
	metadata = MetaData()
	
	myDBDesign = DBDesign()
	
	hostDB = myDBDesign.HostDBDesign(metadata)
	pilotDB = myDBDesign.PilotDBDesign(metadata)
	applicationDB = myDBDesign.ApplicationDesign(metadata)
	appProfileDB = myDBDesign.AppProfileDBDesign(metadata)
	gridTaskDesignDB = myDBDesign.GridTaskDBDesign(metadata)
	PilotResourcesDB = myDBDesign.PilotResourceDBDesign(metadata)
	
	
	#===========================================================================
	# parameterDesignDB = myDBDesign.parameterDBDesign(metadata)
	#===========================================================================

	metadata.create_all(base.engine)
	
	print("Database is correct")
	print("")


	print("Starting connection with GridWay metascheduler")
	ExecutionManager.initGridSession()
	print("Connection stablished")



	print ("Cleaning working directory from past executions")
	ExecutionManager.cleanTmpDirectory()
	print ("... done")

	print ("Selected LRMS")
	print (base.lrms)
	print ("... done")

	print ("Recovering infrastructure from past executions and current state")
	if (base.infrastructureType == None) or (base.infrastructureType == "standard"):
		print ("infrastructur: standard")
		myInfrastructure = Infrastructure.Infrastructure(base.lrms)
	elif (base.infrastructureType == "pilot"):
		myInfrastructure = PilotInfrastructure.PilotInfrastructure(base.lrms)
		print ("infrastructure: pilot")
	elif (base.infrastructureType == "newpilot"):
		myInfrastructure = NewPilotInfrastructure.NewPilotInfrastructure(base.lrms)
		print ("infrastructure: newPilot")	
		
	
	myInfrastructure.load()
	myInfrastructure.showHosts()
	print ("")
	print ("Refreshing certificate (now commented for tests)")
	RefreshCertificates.refreshCertificates(myInfrastructure)
	print ("Infrastructure loaded")

	controller = Controller(infrastructure = myInfrastructure)

	print ("")	
	print ("Preparing infrastructure for the execution of tasks")
	print ("DISABLED; ESTO ES UNA CHAPUZA")
	controller.executeInfrastructureTasks()
	print ("Infrastructure is ready")



	print ("")
	print ("Analyzing application to execute")
	if sys.argv[1] == "recovery":
		controller.recoverExecution()
	else:
		controller.newExecution(requirementsFile)
	print ("Application has been analyzed")
	
	
	print("OK, starting the execution now")
	controller.execute()
	print ("Execution has concluded")
	
	
	print ("")
	print("Closing conection with GridWay metascheduler")
	ExecutionManager.exitGridSession()
	print("Connection closed")
	
	print ("")
	print ("Cleaning output and temporary files")
	print ("COMENTEND FOR DEBUGGIN")
	#shutil.rmtree (base.tmpExecutionFolder)
	print ("Everything clean")
	
	print ("")
	print ("Obtaining execution stats")
	ExecutionStats.executionStats()
	ExecutionStats.executionStats(base.resultFiles)
	print ("Execution status have been obtained")
	
	print("EXECUTION OF MONTERA IS FINISHED NOW")
	

