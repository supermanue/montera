'''
Created on Aug 24, 2012

@author: u5682
'''
from GridTask import GridTask
from Host import Host
from FakeHost import FakeHost
from Pilot import Pilot
from PilotResource import PilotResource

import ExecutionManager
from InformationManager import obtainGWResources

import subprocess as sub
import xml.dom.minidom
from xml.dom.minidom import Node
import os, sys, time
from datetime import datetime, timedelta

from DRMAA import *

import InformationManager
from datetime import datetime

from base import Session
import base
from sqlalchemy import func

class Infrastructure(object):
	'''
	classdocs
	'''


	def __init__(self, lrms = None, hosts  = None):
		'''
		Constructor
		'''
		self.lrms = lrms
		self.hosts = hosts
		
	#this method obtains the infrastructure status as provided from gwps
	#if there is a new host or an existing one has been modified, it creates a profiling task
	#also, the new hosts are added to the host pool (hosts[] array)
	
	
	#TODO: Este nombre es una mierda y no refleja bien su función. Hay que cambiarlo
	def createInfrastructureTasks(self, infrastructureTasks):	
		
		print ("---------------------")
		print ("---------------------")
		print ("---------------------")

		print ("CREATE INFRASTRUCTURE TASKS")
		
					
					
		hostsToProfile = []
		
		hostList =  obtainGWResources()
		for hostInfo in hostList:
			hostName = hostInfo.getElementsByTagName("HOSTNAME")[0].firstChild.data #TODO: remove "unicode" from TEXT
			
			try:
				foundArch = hostInfo.getElementsByTagName("ARCH")[0].firstChild.data
			except:
				foundArch=""
				
			try:	
				foundCpuMHz = int(hostInfo.getElementsByTagName("CPU_MHZ")[0].firstChild.data)
			except:
				foundCpuMHz = 0
			
			try:	
				foundLrms = hostInfo.getElementsByTagName("LRMS_NAME")[0].firstChild.data
			except:
				foundLrms = None
			
			try:	
				freeNodeCount = int(hostInfo.getElementsByTagName("FREENODECOUNT")[0].firstChild.data)
			except:
				freeNodeCount = 0	

			if foundLrms != None:
				if foundLrms == "jobmanager-pilot":			
					#solo tenemos en cuenta los pilots con al menos un slot disponible
					if not freeNodeCount > 0:
						continue
			
			#if a certain LRMS is desired, remove the hosts with a different one
			if self.lrms != None:
				if foundLrms != self.lrms:
					continue
				
			#if host is unknown, create a profiling task
			currentHost = self.getHost(hostName)
			if  currentHost == None:
				newHost = Host(hostName, arch=foundArch, cpuMHz = foundCpuMHz, lrms=foundLrms)
				self.hosts.append(newHost)
				hostsToProfile.append(newHost)
				#store new host on databae (faiulre resistance
				Session.add(newHost)
			#if information has changed, update host information
			elif (currentHost.arch != foundArch) or (currentHost.cpuMHz != foundCpuMHz):
				#TODO: pensar que hacer aqui. habria que eliminar el viejo o solo sobreescribir la información? Si se elimina el viejo, que pasa con las tareas ahí ejecutadas? No es trivial
				currentHost.arch = foundArch
				currentHost.cpuMHz = foundCpuMHz
				if currentHost.lrms == None:
					currentHost.lrms = foundLrms
				hostsToProfile.append(currentHost)
				Session.add(currentHost)
					
			elif currentHost.shouldBeProfiled():
				if currentHost.lrms == None:
					currentHost.lrms = foundLrms
				hostsToProfile.append(currentHost)

				
		#print("Host profiling: submission of 1 tasks per host")		
		hostProfilingTasks = [ExecutionManager.createHostProfilingTask(host) 
							for host in hostsToProfile
							for i in range(1)]
		
		
		
		siteTasks = []
		for task in hostProfilingTasks:
			found=False
			for gridTask in infrastructureTasks:
				if gridTask.host.hostname == task.host.hostname:
					found=True
					break
			if not found:
				siteTasks.append(task)
				
				
		#Esto es para el primer experimento de montera + gwpilot
		#queremos tener pilots funcionando, así que los arranco con esto 
		if self.lrms=="jobmanager-pilot":
			print ("creating fake profiling tasks")
			
			existingFakeTasks = len([task for task in infrastructureTasks if task.host.hostname=="" and task.status != "PENDING"])
			existingGoodPilots = len (self.getGoodHosts())
			existingProfilingTasks = len(hostProfilingTasks)
			#fakeTasksToCreate = base.maxRunningTasks - (existingFakeTasks + existingGoodPilots + existingProfilingTasks)
			fakeTasksToCreate = base.maxRunningTasks - existingFakeTasks
			
			print ("	Desired tasks: " + str(base.maxRunningTasks))
			print ("	Existing fake tasks: " + str(existingFakeTasks))
			print ("	Existing good pilots: " + str(existingGoodPilots))
			print ("	created: " + str(fakeTasksToCreate))
			
			emptyHost = FakeHost()
			fakeHostProfilingTasks = [ExecutionManager.createWakeUpask(emptyHost) 
						for i in range(fakeTasksToCreate)]
	
			siteTasks+=fakeHostProfilingTasks
		
				

		return siteTasks
	
	
	def updateInfoAfterProfiling(self, gridTask):
		print ("Updating info after profiling site " + gridTask.host.hostname)
		print ("	Task info:")
		print  ("	Task id: " + str(gridTask.id))
		print ("	GW ID: " + gridTask.gwID)
		print ("	desired host: " + gridTask.host.hostname)
		print ("	Host type: " + str(gridTask.host.__class__))

		gridTask.status="CLEAR"
		
		#1.- abrir el archivo correspondiente a esa task
		execution_file = base.tmpExecutionFolder + "execution_result_" + gridTask.gwID + ".xml"
		try:
			doc = xml.dom.minidom.parse(execution_file)
		except:
			print("failed when profiling host " + gridTask.host.hostname + ". File " + execution_file + " could not be found")
			gridTask.host.updateInfoAfterFailedProfiling()
			Session.add(gridTask)
			return
		
		executionInfoList = doc.getElementsByTagName('execution_info')
		for executionData in executionInfoList:
			try:
				gridTaskType = executionData.getElementsByTagName("type")[0].firstChild.data #TODO: remove "unicode" from TEXT
				remoteHostName = executionData.getElementsByTagName("hostname")[0].firstChild.data
				whetstones = float(executionData.getElementsByTagName("whetstones")[0].firstChild.data)
				waitingTime = float(executionData.getElementsByTagName("execution_time")[0].firstChild.data)
				dataSize = float(executionData.getElementsByTagName("data_size")[0].firstChild.data)
			except:
				print("failed when profiling host " + gridTask.host.hostname + ". File " + execution_file + " could not be found")
				gridTask.host.updateInfoAfterFailedProfiling()
				Session.add(gridTask)
				return
			
		#2.- procesar los resultados
	
		if gridTaskType != "benchmark":
			print ("ERROR when updating info from a site profiling")
			print("Incorrect task type, readed " + gridTaskType + " and should be \"benchmark\"")
			print ("	considering the execution as failed")
			gridTask.host.updateInfoAfterFailedProfiling()
			Session.add(gridTask)
			return
			
		if remoteHostName != gridTask.host.hostname:
			print ("ERROR when updating info from a site profiling")
			print("Incorrect host name, readed" + remoteHostName + " and should be " + gridTask.host.hostname)
			print ("	considering the execution as failed")
			gridTask.host.updateInfoAfterFailedProfiling()
			Session.add(gridTask)
			return
			
		totalActiveTime = InformationManager.readTotalActiveTime(gridTask.gwID)
		transferTime = totalActiveTime - waitingTime
		queueTime = InformationManager.readQueueTime(gridTask.gwID)
			
		if transferTime > 0:
			bandwidth = dataSize / transferTime
		else:
			bandwidth = -1
		#3.- suministrar esa info al host.
		gridTask.host.updateInfoFromSuccessFulExecution(whetstones, queueTime, bandwidth)
		
		
		#4.- eliminar archivos temporales
		try:
			#os.remove(execution_file)
			print ("IN application.py, I would be deleting " + execution_file)
			print ("Profiling file has been successfully deleted: " + execution_file)
		except:
			print ("Could not delete profiling file " + execution_file)

		gridTask.endDate = datetime.now()
		#clean after execution
		

	
	def getHost(self, hostname):
		foundHost = None
		for host in self.hosts:
			if host.hostname.strip().lower() == hostname:
				foundHost =  host
				break
		if not foundHost:
			print ("Infrastructure. getHost- I am looking for host: ->" + hostname +"<- ... not found :(")
			print ("These are the hosts I have now:")
			self.showHosts()
		return foundHost
	

	
	
	def updateStatus(self, gridTasks):
#===============================================================================
# 		print ("updatestatus: UPDATING INFRASTRUCTURE STATUS")
# 
# 		print("	numbver of tasks to process: " + str(len(gridTasks)))
#===============================================================================

		for host in self.hosts:
			host.currentSlotCount = 0
			
		for gridTask in gridTasks:
			if gridTask.status == "RUNNING":
				try:
					gridTask.host.currentSlotCount +=1
				except:
					pass
				#===============================================================
				# print ("	task running on host " + gridTask.host.hostname  + "with id " + str(gridTask.host.id) +", that makes " + str(gridTask.host.currentSlotCount) + " tasks on the host") 				
				#===============================================================

		for host in self.hosts:
			host.maxSlotCountThisTime = max(host.currentSlotCount, host.maxSlotCountThisTime)
			if host.maxSlotCountThisTime > host.maxSlotCount:
				host.maxSlotCount = host.maxSlotCountThisTime
				Session.add(host)
				#===============================================================
				# print ("updateStatus. updated information: host " + host.hostname + " now has " + str(host.maxSlotCount) + " slots")
				#===============================================================
	
	#devuelve una lista de los hosts que sabemos que funcionan			
	def getGoodHosts(self):
		print ("GET GOOD HOSTS.")
		pastHostsList = [host for host in self.hosts if host.successfulExecutions > 0 ]
		hostList = obtainGWResources()

		#load XML to memory,  and extract data from hosts
		presentHostNames = []

		for hostInfo in hostList:
			hostName = hostInfo.getElementsByTagName("HOSTNAME")[0].firstChild.data #TODO: remove "unicode" from TEXT
			presentHostNames.append(hostName)
		
		for host in pastHostsList:
			found = False
			for name in presentHostNames:
				if name == host.hostname:
					found = True
					break
			if not found:
				pastHostsList.remove(host)
			if host.lrms != self.lrms:
				pastHostsList.remove(host)

		#banning failure hosts
		for host in pastHostsList:
			bannedTime = None
			if host.failedProfilings < 0:
				continue
			elif host.failedProfilings == 0 and host.successfulExecutions > 0:
				continue

			#24 primeros fallos: banning de una hora por fallo
			if host.failedProfilings < 24:
				bannedTime =timedelta(hours=host.failedProfilings)
			elif  host.failedProfilings >= 24:
				bannedTime = timedelta(days=7)
			if bannedTime != None:	
				if (datetime.now() - bannedTime) < host.lastFailedProfiling:
					pastHostsList.remove(host)
# 					print ("	Host "+ host.hostname + " is banned due to failures for " + str(bannedTime))

		#ban hosts with no whetstones, what means no profiling
		hostsWithWhetstones = []
		for host in pastHostsList:
			if host.getWhetstones() > 1:
				hostsWithWhetstones.append(host)
# 			else:
# 				print ("	Host "+ host.hostname + " is banned, no whetstone info")

		
		#Un poco chapuza esto, pero bueno
		defList=[]
		if self.lrms == "jobmanager-pilot":
			print ("	filtering hosts.")
			for host in hostsWithWhetstones:
				if host.lrms == self.lrms:
					defList.append(host)
					print ("		keeped host " + host.hostname + " with lrms=" + host.lrms)
				else:
					print ("		deleted host " + host.hostname + " with lrms=" + host.lrms)
		else:
			defList = hostsWithWhetstones
			
		print ("	Returning list of size " + str(len(defList)))
		for host in defList:
			print ("		" + host.hostname)
		return defList
	
	
	#Loads hosts from database
	#as an additional step, it checks that the hosts still exist. this avoids submitting tasks 
	#to non existing hosts, as those from a different VO where Montera had been previoysly employed
	def load(self):
		pastHosts = Session.query(Host).all()

		print ("INFRASTRUCTURE-LOAD: LOADING INFORMATION FROM PAST EXECUTIONS")
		print ("	Desired LRMS: " + self.lrms)
		#obtain present hosts prom gridway
		
				#PILOTS
		pastPilots = Session.query(Pilot).all()
		print ("	Deleting all past pilots from database")
		for pilot in pastPilots:
			base.Session.delete(pilot)
			
			
		presentHosts = []
		GWHosts =  obtainGWResources()

		if GWHosts == []:
			print("Error when parsing host information file, employing information from past executions")	
			self.hosts = pastHosts
			return

	
		#load XML to memory and extract data from hosts
		for hostInfo in GWHosts:
			hostName = hostInfo.getElementsByTagName("HOSTNAME")[0].firstChild.data #TODO: remove "unicode" from TEXT
			
			#for every found host, check if it existed on a previous execution
			for host in pastHosts:
				if host.hostname.strip().lower() == hostName.strip().lower():
					
					#in the case of pilot jobs, only employ the ones with FREENODECOUNT > 0
					if host.lrms =="jobmanager-pilot":
						try:	
							freeNodeCount = int(hostInfo.getElementsByTagName("FREENODECOUNT")[0].firstChild.data)
						except:
							freeNodeCount = 0
						if not freeNodeCount > 0:
							continue;
					
					presentHosts.append(host)
					break  # Si no pones este break, y está repetido en memoria, se cargan todos

		
		#now, if only hosts with a certain LRMS are desired, we remove the rest
		if self.lrms != None:
			for host in presentHosts:
				if host.lrms != self.lrms:
					print ("	Removing host " + host.hostname + ", wrong LRMS found: " + host.lrms)
					presentHosts.remove(host)
				else:
					print ("	Keeping host " + host.hostname)
		
		#the hosts that we will first employ are the resulting ones
		self.hosts = presentHosts
		
		
	def showHosts(self):
		
		print ("...........")
		print ("Showing host list")
		
		print ("---Database Info")
		total = base.Session.query(Host.id).count()
		print ("Total number of hosts: " + str(total))
		
		hosts = base.Session.query(Host.hostname, func.count(Host.id)).group_by(Host.hostname).order_by(Host.hostname)
		
		for host in hosts:
			print (host[0] + ": " + str(host[1]))
				
		print ("---Memory Info")
		total = len(self.hosts)
		print ("Total number of hosts: " + str(total))

		for host in self.hosts:
			print (host.hostname)
		print ("Total number of hosts in memory: " + str(total))

		print ("---") 
		
		
	def getSiteWaitingtime(self):
		return 0
		#=======================================================================
		# if len(self.hosts) == 0:
		# 	return 1000
		# else:
		# 	return 0
		#=======================================================================
		
