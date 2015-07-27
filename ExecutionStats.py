'''
Created on Feb 20, 2013

@author: u5682
'''
'''
Created on Aug 20, 2012

@author: Manuel Rodríguez Pascual, <manuel.rodriguez.pascual@gmail.com>

	This file is part of Montera.

	gridwayController is free software: you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	gridwayController is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with gridwayController.  If not, see <http://www.gnu.org/licenses/>.
'''

from sqlalchemy import *

import sys, os

from base import Session
from base import Base, Session, engine

from datetime import datetime, timedelta

from DBDesign import DBDesign
from Host import Host
from Controller import Controller
from Application import Application
from GridTask import GridTask

import UserInterface
import ExecutionManager


def executionStats(fileLocation= None):

	
	
	metadata = MetaData()
	
	myDBDesign = DBDesign()
	
	hostDB = myDBDesign.HostDBDesign(metadata)
	applicationDB = myDBDesign.ApplicationDesign(metadata)
	appProfileDB = myDBDesign.AppProfileDBDesign(metadata)
	gridTaskDesignDB = myDBDesign.GridTaskDBDesign(metadata)
	
	metadata.create_all(engine)
	
	#load application
	myApp = Session.query(Application).order_by(Application.id.desc()).first()
	
	
		
	if fileLocation != None:
		fileLocation = fileLocation + "/" + str(myApp.id) + "_execution_stats.txt"
		f = open(fileLocation, 'wb')
		sys.stdout = f


	print("WELLCOME TO MONTERA 2.0")
	print("---")
	print ("date:" + str(datetime.now()))
	print ("Recovering stats from last execution")
	
	
	print("")
	print ("ID: " + str(myApp.id))
	print ("Application: " + myApp.name)
	print ("Number of desired samples: " + str(myApp.desiredSamples))
	
	
	
	#load desired tasks
	taskList = Session.query(GridTask).filter(myApp.id == GridTask.applicationID).all()

	#find first task
	taskCreations = [x.creationDate for x in taskList]
	firstTaskCreation = reduce (lambda x, y: min(x, y), taskCreations)
	lastTaskCompletion = timedelta(0)

	
	print ("TASK_ID	Status	Samples	Creation time	queue time	Execution time	Host	Host_whetstones")

	for task in taskList:
		
		taskInit = max(task.creationDate, firstTaskCreation)
		
		#si ha sido creada antes de enviar la primera lo consideramos cero
		creationTime = taskInit - firstTaskCreation
		#=======================================================================
		# if creationTime > timedelta(0):
		#	print ("mas que cero")
		#=======================================================================
		

		try:   
			waitingTime = task.executionStartDate - task.creationDate
		except:
			waitingTime = timedelta(0)
			
		try:
			executionTime = task.endDate - task.executionStartDate
			lastTaskCompletion = max(lastTaskCompletion, waitingTime + executionTime)
		except:
			executionTime = timedelta(0)
			
			
		print (str(task.id) + "	" + task.status + "	" + str(task.maxSamples) + "	"+ str(creationTime.seconds)  + "	" + str(waitingTime.seconds) + "	" + str(executionTime.seconds) + "	" + task.host.hostname + "	" + str(task.host.getWhetstones()))


	print ("Execution stats")
	
	taskNumber = len(taskList)
	submittedTasks = len([ x for x in taskList if x.status=="SUBMITTED"] )
	runningTasks = len([ x for x in taskList if x.status=="RUNNING"] )
	clearedTasks =  len([ x for x in taskList if x.status=="CLEAR"] )
	
	print ("-----")
	print ("Task stats: ")
	print ("	Number of created tasks:	" + str(taskNumber))
	print ("	Queued tasks:	" + str(submittedTasks))
	print ("	Running tasks:	" + str (runningTasks))
	print ("	Cleared tasks:	" + str (clearedTasks))
	print ("	Walltime:	" + str(lastTaskCompletion))
	print ("	Walltime Seconds:	" + str(lastTaskCompletion.days * 86400 + lastTaskCompletion.seconds))
	
	createdTaskSamplesList = [x.maxSamples for x in taskList]
	try:
		createdTaskSamples = reduce (lambda x, y: x + y, createdTaskSamplesList)
	except:
		createdTaskSamples = 0
			
	runningTaskSamplesList = [x.maxSamples for x in taskList if x.status =="RUNNING"]
	try:
		runningTaskSamples = reduce (lambda x, y: x + y, runningTaskSamplesList)
	except:
		runningTaskSamples = 0
		
	endedTaskSamplesList = [x.realSamples for x in taskList if x.status =="CLEAR"]
	try:
		endedTakSamples = reduce (lambda x, y: x + y, endedTaskSamplesList)
	except:
		endedTakSamples =0
	
	
	
	submittedSamples = runningTaskSamples + endedTakSamples
	
	print ("")
	print ("Sample stats")
	print ("	Created samples:	" + str(createdTaskSamples))
	print ("	Submitted samples:	" + str(submittedSamples))
	print ("	Ended samples:	" + str(endedTakSamples))
	
	
	if fileLocation != None:
		f.close()
		os.chmod(fileLocation, 0777)

if __name__ == '__main__':
	executionStats()
	
	
