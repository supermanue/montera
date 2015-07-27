'''
Created on Aug 24, 2012

@author: u5682
'''

import InformationManager
import ExecutionManager
import UserInterface


from Infrastructure import Infrastructure
from PilotInfrastructure import PilotInfrastructure
from Application import Application
from GridTask import GridTask
from DyTSS import DyTSS

from time import sleep
from datetime import datetime
import sys, os
import base

from sqlalchemy import or_
from sqlalchemy.orm import sessionmaker, scoped_session

class Controller(object):
	'''
	classdocs
	'''


	def __init__(self, infrastructure = None, 
				application = None, 
				schedulingAlgorithm = None):
		'''
		Constructor
		'''
	
		self.myInfrastructure = infrastructure
		self.myApplication = application
		self.mySchedulingAlgorithm = schedulingAlgorithm
		self.applicationTasks = []
		self.infrastructureTasks = []
		


	def execute(self):
		
		while True: 
			#update status of tasks being executed
			print("---")
			print ("date:" + str(datetime.now()))


			print("")
			print ("CHECKING FOR GRID CERTIFICATE")
			if not InformationManager.checkForValidCertificates():
				print ("Could not find a valid certificate")
				print ("Finishing execution now :(")
				break

			#===================================================================
			# print ("PRINCICIPIO DE EXECUTE")
			# for task in self.infrastructureTasks:
			# 	print ("Task " + task.gwID + "has hosttype " +  str(task.host.__class__))
			# 	
			# 	
			#===================================================================

			print ("")
			print ("UPDATING INFRASTRUCTURE STATUS")
			for gridTask in self.infrastructureTasks:
				ExecutionManager.updateGridTaskStatus(gridTask)
				
			finishedTasks = [gridTask for gridTask in self.infrastructureTasks if gridTask.status=="DONE"]
			for gridTask in finishedTasks:
				if gridTask.type == "hostProfiling":
					self.myInfrastructure.updateInfoAfterProfiling(gridTask)
				#and update gridTask status 
				ExecutionManager.removeTaskFromGW(gridTask)
				self.infrastructureTasks.remove(gridTask)
				
			self.myInfrastructure.updateStatus(self.infrastructureTasks)
	
			print ("")
			print ("CREATING INFRASTRUCTURE TASKS")
			self.executeInfrastructureTasks()
		
		
		
			#===================================================================
			# print ("FILNAL DE EXECUTE")
			# for task in self.infrastructureTasks:
			# 	print ("Task " + task.gwID + "has hosttype " +  str(task.host.__class__))
			#===================================================================


		
			print ("UPDATE APPLICATION STATUS")
			for gridTask in self.applicationTasks:
				ExecutionManager.updateGridTaskStatus(gridTask)
			
			#estoe s una chapuza
			totalTasks = self.infrastructureTasks + self.applicationTasks

			self.myInfrastructure.updateStatus(totalTasks)

			#process recently finishedTasks
			finishedTasks = [gridTask for gridTask in self.applicationTasks if gridTask.status=="DONE"]
			for gridTask in finishedTasks:
				if gridTask.type == "applicationProfiling":
					self.myApplication.updateInfoAfterProfiling(gridTask)		
				elif gridTask.type == "applicationExecution":
					self.myApplication.updateInfoAfterExecution(gridTask)

				#and update gridTask status 
				ExecutionManager.removeTaskFromGW(gridTask)
				self.applicationTasks.remove(gridTask)
				
			#check for execution finish
			if self.myApplication.remainingSamples <= 0:
				print ("Starting the exit of  execution loop")

				#TODO: poner en los hosts que el máximo de host total es el maximo del total y del maximoThisTime
				self.myApplication.finished = 1
				base.Session.add(self.myApplication)
				
				print ("Application marked as finished")

				print ("")
				print ("Storing information about available hosts on remote sites")
				for host in self.myInfrastructure.hosts:
					host.maxSlotCount = max(host.maxSlotCount, host.maxSlotCountThisTime)
					base.Session.add(host)
					
				print ("")
				print ("Removing finished tasks from gridWay")	
				
				#esto es discutible,
				#ahora, si una tarea de profiling ha llegado hasta el final de la ejecución sin ser completada, la marco como fallida
				#esto perjudica a los sitios que tienen un tiempo enorme de respuesta
				#lo contrario hace que si un sitio no contesta se siga considerando pendiente de profiling hasta el infinito
				
				for gridTask in self.applicationTasks:
					ExecutionManager.removeTaskFromGW(gridTask)
					if gridTask.type == "hostProfiling":
						self.myInfrastructure.updateInfoAfterProfiling(gridTask)
				try: 
					base.Session.commit()
				except:
					base.Session.rollback()
					print ("Lost connection with database, not storing anything!")
					
				print ("Exiting execution loop")
				break
		
			
			print ("")
			print ("CREATING NEW EXECUTION TASKS")
			#===================================================================
			# self.myInfrastructure.showHosts()
			#===================================================================
			applicationExecutionTasks = self.mySchedulingAlgorithm.createApplicationTasks(self.myInfrastructure, self.myApplication, self.applicationTasks)
			for gridTask in applicationExecutionTasks:
				ExecutionManager.submit(gridTask)
				self.applicationTasks.append(gridTask)
				base.Session.add(gridTask)
			
			try: 
				base.Session.commit()
			except:
				base.Session.rollback()
				print ("Lost connection with database, not storing anything!")
			
			print("...")	
			sleep(15)
			
	
	def purgeDB(self):
		
		for unfinishedApplication in base.Session.query(Application).filter(Application.finished==False):
			unfinishedApplication.finished= True
			base.Session.add(unfinishedApplication)
			
		for unfinishedTask in base.Session.query(GridTask).filter(GridTask.status!="CLEAR"):
			unfinishedTask.status = "CLEAR"
			base.Session.add(unfinishedTask)
		
		try: 
			base.Session.commit()
		except:
			base.Session.rollback()
			print ("Lost connection with database, not storing anything!")
	

			
	def profileApplication(self):
		
		goodHosts = self.myInfrastructure.getGoodHosts()
		appProfilingTasks = [ ExecutionManager.createProfilingTask(host, self.myApplication)for host in goodHosts for i in range(2)]
		#appProfilingTasks = [self.myApplication.profile.createProfilingTask(host) for host in goodHosts]
		
		
		#DEBIG
		#=======================================================================
		# profilingTask = appProfilingTasks[0]
		# profilingTask.gwID = "2217"
		# self.myApplication.updateInfoAfterProfiling(profilingTask)
		#=======================================================================
		
				#submit the tasks to the Grid
		for profilingTask in appProfilingTasks:
			ExecutionManager.submit(profilingTask)
			base.Session.add(profilingTask)
	
		try: 
			base.Session.commit()
		except:
			base.Session.rollback()
			print ("Lost connection with database, not storing anything!")
			
		profilingTime = 10 * self.myApplication.maxProfilingTime
		print ("Waiting for the app profiles to finish, waiting for " + str(profilingTime) + " seconds")
		ExecutionManager.waitForTermination(appProfilingTasks, waitingTime = profilingTime)
				
		for profilingTask in appProfilingTasks:
			self.myApplication.updateInfoAfterProfiling(profilingTask)		
		
		try: 
			base.Session.commit()
		except:
			print ("Lost connection with database, trying to recover it")
			try:
				base.Session = scoped_session(sessionmaker(bind=base.engine))
				for profilingTask in appProfilingTasks:
					base.Session.add(profilingTask)
					base.Session.commit()
				print ("Worked, yeah")
			except:
				print ("didn't work, not storing anything! Will probably crash soon LOL")	

			base.Session.rollback()
			
			print ("Lost connection with database, not storing anything!")


	def newExecution(self, requirementsFile):
		
		print("")
		print("Starting the execution of a new Montera template")
		print("")

		print ("")
		print ("Cleaning the DB from past executions")
		self.purgeDB()
		print ("Database cleaned")

		print ("")
		print ("Reading application requirements")
			
		self.myApplication= UserInterface.readRequirements(requirementsFile)
		base.Session.add(self.myApplication.profile)	
		base.Session.add(self.myApplication)

		base.Session.commit()
		#TODO: esto es un añapa, pero el base.Session.commit jode este puntero
		self.myApplication.profile.application = self.myApplication
		print ("application requirements read an stored")
			
		print ("")
		print ("name: " + self.myApplication.name)
		print ("desired samples: " + str(self.myApplication.desiredSamples))
		
		
		#base.tmpExecutionFolder += str(self.myApplication.id) + "/"
		
		try:
			os.makedirs(base.tmpExecutionFolder)
			print ("Creating temporary folder for application results, " + base.tmpExecutionFolder)
		except:
			print ("Temporary folder for the application already exists, ok")
		
		
		print("")
		print("Checking if a profiling of the application is neccesary")
		if self.myApplication.profile.numProfilings == 0:
			print("it is necessary. Starting app profile!")
			self.profileApplication()
			try: 
				base.Session.commit()
			except:
				base.Session.rollback()
				print ("Lost connection with database, not storing anything!")
			print("Application profiling finished")
		else:
			print("Application profiling loaded from past executions, profiling is not needed")
		print ("Constant effort, sample effort:")
		print (str(self.myApplication.profile.constantEffort) + " whetstones, " + str(self.myApplication.profile.sampleEffort) + " whetstones/sample")

		
		print("")
		print ("Loading scheduling algorithm")
		self.mySchedulingAlgorithm =InformationManager.loadSchedulingAlgorithm(self.myApplication.schedulingAlgorithm)
		if (self.mySchedulingAlgorithm != None):
			print ("Scheduling Algorithm loaded")
		else:
			print ("Could not find an appropriate scheduling algorithm")
			sys.exit(1)

		#este metodo es la ostia, recupera una ejecución pasada
	def recoverExecution(self):
		print("")
		print("Starting recovery from past execution")
	


		print("")
		print("Loading application to execute")
		self.myApplication = base.Session.query(Application).filter(Application.finished=="0").first()
		if self.myApplication == None:
			print("Could not load application, exiting")
			sys.exit(1)
		
# 		base.tmpExecutionFolder += str(self.myApplication.id) + "/"
		try:
			os.makedirs(base.tmpExecutionFolder)
		except:
			print ("Temporary folder for the application already exists, ok")		
			
		self.myApplication.calculateInputFileSize()
		#TODO: esto es un añapa, pero no sé mejor como cargarlo
		self.myApplication.profile.application = self.myApplication
		print("Application Loaded")

	
		print("")
		print ("Loading scheduling algorithm")
		self.mySchedulingAlgorithm =InformationManager.loadSchedulingAlgorithm(self.myApplication.schedulingAlgorithm, initialized=True)
		if (self.mySchedulingAlgorithm != None):
			print ("Scheduling Algorithm loaded")
		else:
			print ("Could not find an appropriate scheduling algorithm")
			sys.exit(1)
		print("")
		
		print("Loading tasks being executed")
		self.applicationTasks = base.Session.query(GridTask).filter(GridTask.status!="CLEAR", \
																or_(GridTask.applicationID == self.myApplication.id,GridTask.applicationID == None), \
																or_(GridTask.type=="applicationExecution",GridTask.type=="applicationProfiling")).all()

		self.infrastructureTasks  = base.Session.query(GridTask).filter(GridTask.status!="CLEAR", \
																or_(GridTask.applicationID == self.myApplication.id,GridTask.applicationID == None), \
																or_(GridTask.type=="hostProfiling",GridTask.type=="pilot")).all()
		print("Tasks loaded")
		
		
		
	def executeInfrastructureTasks(self):

		print ("")
		print("Executing Infrastructure tasks")
		print ("	...Executing site tasks")


		#ESTO ES UNA CHAPUZA
		#para saber si hay que hacer un profiling completo o no, miro la infraestructura
		#si no tiene hosts es que toca.
		#entonces, lo miro antes de crear las tarea,s ya que al crear las tareas 
		#se rcean loss hosts...
					
		waitingTime = self.myInfrastructure.getSiteWaitingtime()
		siteTasks = self.myInfrastructure.createInfrastructureTasks(self.infrastructureTasks)

		for task in siteTasks:
			ExecutionManager.submit(task)
			#===================================================================
			# print ("desactivado el envio de tareas de profiling: ID: PERROLOCO")
			#===================================================================
			self.infrastructureTasks.append(task)
			
		
		if (waitingTime > 0):
			print("Waiting for profiling tasks to execute")
			print("this will take " + str(waitingTime) + " seconds to execute")

			ExecutionManager.waitForTermination(siteTasks, waitingTime=waitingTime)
			print("profiling tasks executed")
	
			for task in siteTasks:
				self.myInfrastructure.updateInfoAfterProfiling(task)		
				task.status = "CLEAR"
				base.Session.add(task)
				

