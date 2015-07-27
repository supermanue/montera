from sqlalchemy import *

import base

from DBDesign import DBDesign
from Application import Application
from GridTask import GridTask
import ExecutionManager
import PilotResource

def analyzeProfile(fileLocation= None):

	
	
	metadata = MetaData()
	
	myDBDesign = DBDesign()
	
	hostDB = myDBDesign.HostDBDesign(metadata)
	applicationDB = myDBDesign.ApplicationDesign(metadata)
	appProfileDB = myDBDesign.AppProfileDBDesign(metadata)
	gridTaskDesignDB = myDBDesign.GridTaskDBDesign(metadata)
	
	metadata.create_all(base.engine)
	
	print("Starting connection with GridWay metascheduler")
	ExecutionManager.initGridSession()
	print("Connection stablished")
	
	
	
	#load application
	myApp = base.Session.query(Application).order_by(Application.id.desc()).first()
	
	base.tmpExecutionFolder = base.tmpExecutionFolder + "/" + str(myApp.id) + "/"
	
	myTasks = base.Session.query(GridTask).filter(GridTask.applicationID==myApp.id)
	
	for task in myTasks:
		ExecutionManager.updateGridTaskStatus(task)
				
	finishedTasks = [gridTask for gridTask in myTasks if gridTask.status=="DONE"]
	for gridTask in finishedTasks:
		if gridTask.type == "applicationProfiling":
			myApp.updateInfoAfterProfiling(gridTask)

	print ("")
	print("Closing conection with GridWay metascheduler")
	ExecutionManager.exitGridSession()
	print("Connection closed")
	

if __name__ == '__main__':
	analyzeProfile()
	