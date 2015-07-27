'''
Created on Aug 24, 2012

@author: u5682
'''
from GridTask import GridTask
from Host import Host
from Pilot import Pilot
import ExecutionManager
from Infrastructure import Infrastructure
from PilotResource import PilotResource
from InformationManager import obtainGWResources
from math import ceil
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


class PilotInfrastructure(Infrastructure):
	'''
	classdocs
	'''


	def __init__(self, *args, **kwargs):
		super(PilotInfrastructure, self).__init__()
		
		self.pilots = None
	
		#HOST: pilots corriendo, que pueden ejecutar trabajos. Se llaman pilot_X 
		#SITE: sitio remoto donde se ejecutan los pilots, 
		#Resource: pilot o site. 
	
	
	def getHost(self, hostname):
		foundHost = None
		for pilot in self.pilots:
			if pilot.hostname.strip().lower() == hostname:
				foundHost =  pilot
				break
		for host in self.hosts:
			if host.hostname.strip().lower() == hostname:
				foundHost =  host
				break	
		return foundHost
	

	

	#PRIMERO ACTUALIZAMOS EL ESTADO DE LOS PILOTS Y LUEGO EL DE LOS HOTSTS
	def updateStatus (self, gridTasks):


		print ("UPDATE STATUS")
		GWHosts = obtainGWResources()
		if GWHosts ==[]:
			print("Error when parsing host information file, employing information from past executions")	
			return GWHosts
		#PILOTS
		pastPilots = Session.query(Pilot).all()
		presentPilots = []
	
		#load XML to memory and extract data from hosts
		for resource in GWHosts:
			try:	
				foundLrms = resource.getElementsByTagName("LRMS_NAME")[0].firstChild.data.strip().lower()
			except:
				print ("Could not obtain resource LRMS, skipping it")
				continue
			
			if foundLrms == "jobmanager-pilot":
				hostname = resource.getElementsByTagName("HOSTNAME")[0].firstChild.data.strip().lower() #TODO: remove "unicode" from TEXT
				#numero de nodos libres. Esto en los pilots funciona bien, y es lo que se emplea para saber si está
				#activo o apagado
				try:	
					freeNodeCount = int(resource.getElementsByTagName("FREENODECOUNT")[0].firstChild.data)
				except:
					freeNodeCount = 0
				if not freeNodeCount > 0:
					continue;

				for pilot in pastPilots:
					if pilot.hostname == hostname:
						presentPilots.append(pilot)
						break	

		#the pilots that we will first employ are the resulting ones
		self.pilots = presentPilots
		
		#AHORA EL DE LOS HOSTS
		runningPilots = [pilotJob for pilotJob in gridTasks
 							if pilotJob.type=="pilot"]
		
		for host in self.hosts:
			host.currentSlotCount = 0
			
		for pilotJob in runningPilots:
			if pilotJob.status == "RUNNING":
				#site = self.getSite(pilotJob.host.hostname)
				host = pilotJob.host
				host.currentSlotCount +=1
				
		for host in self.hosts:
			host.maxSlotCountThisTime = max(host.currentSlotCount, host.maxSlotCountThisTime)
			if host.maxSlotCountThisTime > host.maxSlotCount:
				host.maxSlotCount = host.maxSlotCountThisTime
				Session.add(host)
	
		
		
		
								
	
	#devuelve una lista de los hosts que sabemos que funcionan	
	#AQUI SOLO DEVUELVO PILOTS		
	def getGoodHosts(self):
		goddHosts=[]
		for pilot in self.pilots:
			if pilot.getWhetstones() > 0:
				goddHosts.append(pilot)
		return goddHosts
	
	
	#Loads hosts from database
	#as an additional step, it checks that the hosts still exist. this avoids submitting tasks 
	#to non existing hosts, as those from a different VO where Montera had been previoysly employed
	
	#in this case we load two things: SITES and PILOTS
	
	#To load pilots, what we do is to clear previous state and the create all pilots, 
	#represented as a relationship between the host and the resource. host: hostname, resource: site, worker node
	
	def load(self):
		
		print ("INFRASTRUCTURE-LOAD: LOADING INFORMATION FROM PAST EXECUTIONS")	
		
		#SITES
		pastHosts = Session.query(Host).filter(Host.type=="hosts").all()
		presentHosts = []
		
		#PILOTS
		pastPilots = Session.query(Pilot).all()
		print ("	Deleting all past pilots from database")
		for pilot in pastPilots:
			base.Session.delete(pilot)
				
		presentPilots = []
	
		GWHosts = obtainGWResources()
		print ("Updating information")
		#load XML to memory and extract data from hosts
		try: 
			for resource in GWHosts:
				
				hostname = resource.getElementsByTagName("HOSTNAME")[0].firstChild.data.strip().lower() #TODO: remove "unicode" from TEXT
				try:	
					foundLrms = resource.getElementsByTagName("LRMS_NAME")[0].firstChild.data
				except:
					print ("Could not obtain resource LRMS for site " + hostname + ", skipping it")
					continue
				
				if foundLrms == "jobmanager-pilot":
					#nombre de pilot
					genericArgs = resource.getElementsByTagName("GENERIC_VAR_STR")
					for node in genericArgs:
						if node.attributes['NAME'].value =="PILOT_REAL_HOSTNAME":
							workerNode = node.attributes['VALUE'].value.strip().lower()
						if node.attributes['NAME'].value =="PILOT_REAL_RESOURCE":
							site = node.attributes['VALUE'].value.strip().lower()
						
					#numero de nodos libres. Esto en los pilots funciona bien, y es lo que se emplea para saber si está
					#activo o apagado
					try:	
						freeNodeCount = int(resource.getElementsByTagName("FREENODECOUNT")[0].firstChild.data)
					except:
						freeNodeCount = 0
					if not freeNodeCount > 0:
						continue;

					#cargamos el pilot en la base de datos:
					#primero buscamos el pilotResource, o lo creamos  si no existe
					#después creamos el pilot que emplee ese recurso
					
					pilotResource = base.Session.query(PilotResource).filter(PilotResource.site == site, PilotResource.workerNode == workerNode).first()
					
					if pilotResource == None:
						pilotResource = PilotResource(site, workerNode)
					
					pilot = Pilot(hostname, pilotResource=pilotResource)
					presentPilots.append(pilot)

				else:	#it is a site	
					for host in pastHosts:
						if host.hostname == hostname:
							presentHosts.append(host)
							break		
		
		except:	
			print("	Error when parsing host information file, employing information from past executions")	
			self.hosts = pastHosts
			self.pilots = []
			return
			
		
		#the hosts that we will first employ are the resulting ones
		self.pilots = presentPilots
		self.hosts = presentHosts
		
		
	def showHosts(self):
		
		print ("...........")
		print ("Showing host list")
		#=======================================================================
		# 
		# print ("---Database Info")
		# total = base.Session.query(Host.id).count()
		# print ("Total number of Pilots: " + str(total))
		# 
		# Pilots = base.Session.query(Host.hostname, func.count(Host.id)).group_by(Host.hostname).order_by(Host.hostname)
		# 
		# for host in hosts:
		# 	print (host[0] + ": " + str(host[1]))
		# 		
		# print ("---Memory Info")
		#=======================================================================
		total = len(self.pilots)
		print ("Total number of Pilots: " + str(total))
		total = len(self.hosts)
		print ("Total number of Hosts: " + str(total))
		
		print ("")
		print ("Hosts: ")
		for site in self.hosts:
			print (site.hostname)
			
		print ("")
		print ("Pilots:")
		for host in self.pilots:
			print (host.hostname)
			
		print ("---") 
		
	
	def createPilotTasks(self, infrastructureTasks):
		
		pilots = []
		
		print ("CREATE PILOT TASKS")
		totalRunningPilots = 0
		for host in super(PilotInfrastructure, self).getGoodHosts():
			print ("Host: " + host.hostname)
			
			#para cada sitio, cojo el máximo de pilots
			maxslots = int(ceil(host.maxSlotCount * 1.20)) #ponemos un 20% más para que siempre haya encolados en el site y así detectemos su tamaño real
			
			numberOfPilotsOnTheSite = min(maxslots, base.maxPilotsPerSite)
			
			#miro cuantos hay en cola
			queuedPilots =0
			runningPilotTask = 0
			for task in infrastructureTasks:
				if task.type =="pilot" and  task.status !="RUNNING" and task.host.hostname == host.hostname:
					queuedPilots +=1
				elif task.type =="pilot" and  task.status =="RUNNING" and task.host.hostname == host.hostname:
					runningPilotTask +=1

			#miro cuantos pilots hay ejecutandose ahi
			runningPilots = 0
			for pilot in self.pilots:
				if pilot.pilotResource.site == host.hostname:
						runningPilots +=1
			
			runningPilots = max(runningPilots, runningPilotTask)
			pilotsToExecute = numberOfPilotsOnTheSite - runningPilots - queuedPilots

			print ("	Site: "+ host.hostname)
			print ("		max number of pilots: "+ str(maxslots))
			print ("		Manuel limit (hardcoded): " + str(base.maxPilotsPerSite))		
			print ("		queued number of pilots: " + str(queuedPilots))
			print ("		current number of running pilots according to gwps:" + str (runningPilotTask))
			print ("		current number of running pilots according to gwhosts: " + str(runningPilots))
			print("			number of pilots to execute: "+ str(pilotsToExecute))


			
			for i in range(pilotsToExecute):
				pilots.append(ExecutionManager.createPilotTask(host))
			
			totalRunningPilots += runningPilots
			
			
			
		#not, in "pilots" we have all pilots we want to submit. What we do is priorize them by site.
		print ("Priorizing pilots to submit")
		
		self.priorize(pilots)
		pilotsToSubmit = base.maxRunningTasks - totalRunningPilots
		print ("	total pilots allowed: " + str(base.maxRunningTasks))
		print ("	total running pilots: " + str(totalRunningPilots))
		print ("	pilots to submit: " + str(pilotsToSubmit))	
		
		if (pilotsToSubmit > 0):
			pilots=pilots[:pilotsToSubmit]
		else:
			pilots=[]	

		return pilots
	
	
	#TODO: Este nombre es una mierda y no refleja bien su función. Hay que cambiarlo
	def createInfrastructureTasks(self, infrastructureTasks):				
		hostList = obtainGWResources()
		
		hostsToProfile = []

		print ("createInfrastructureTasks- PilotInfrastructure")
		print ("Analyzing resources ")
		for hostInfo in hostList:
			hostName = hostInfo.getElementsByTagName("HOSTNAME")[0].firstChild.data.strip().lower() #TODO: remove "unicode" from TEXT
			
			try:
				foundArch = hostInfo.getElementsByTagName("ARCH")[0].firstChild.data
			except:
				foundArch=""
				
			try:	
				foundCpuMHz = int(hostInfo.getElementsByTagName("CPU_MHZ")[0].firstChild.data)
			except:
				foundCpuMHz = 0
			
			try:	
				foundLrms = hostInfo.getElementsByTagName("LRMS_NAME")[0].firstChild.data.strip().lower()
			except:
				foundLrms = None
				print ("Could not find LRMS for host " + hostName + ", skipping it")
				continue
			
			try:	
				freeNodeCount = int(hostInfo.getElementsByTagName("FREENODECOUNT")[0].firstChild.data)
			except:
				freeNodeCount = 0	


			if foundLrms == "jobmanager-pilot":			
				#solo tenemos en cuenta los pilots con al menos un slot disponible
				if not freeNodeCount > 0:
					continue
				
				genericArgs = hostInfo.getElementsByTagName("GENERIC_VAR_STR")
				for node in genericArgs:
					if node.attributes['NAME'].value =="PILOT_REAL_HOSTNAME":
						workerNode = node.attributes['VALUE'].value.strip().lower()
					if node.attributes['NAME'].value =="PILOT_REAL_RESOURCE":
						site = node.attributes['VALUE'].value.strip().lower()
							
			
			#if a certain LRMS is desired, remove the hosts with a different one
			if self.lrms != None:
				if foundLrms != self.lrms:
					continue
				
			#if host is unknown, create a profiling task
			currentHost = self.getHost(hostName)
			if  currentHost == None:
				print ("Host/Pilot  not found. hostname: " + hostName + ", LRMS: " + foundLrms)
				if foundLrms == "jobmanager-pilot":
					#he encontrado un pilot:
					#primero busco e resource, y si no existe lo creo.
					#luego creo un pilot que utilice ese resource

					pilotResource = base.Session.query(PilotResource).filter(PilotResource.site == site, PilotResource.workerNode == workerNode).first()
					if pilotResource == None:
						print ("PilotResource was not found, creating a new one")
						pilotResource = PilotResource(site, workerNode)
					print ("Creating a new Pilot in PilotInfrastructure.createInfrastructureTasks")
					newHost = Pilot(hostName, arch=foundArch, cpuMHz = foundCpuMHz, pilotResource = pilotResource)
					self.pilots.append(newHost)
				else:
					print ("Creating a new Host in PilotInfrastructure.createInfrastructureTasks")
					newHost = Host(hostName, arch=foundArch, cpuMHz = foundCpuMHz, lrms=foundLrms)
					self.hosts.append(newHost)
					
				print ("Host to profile: " + hostName + ": NEW ")
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
				#===============================================================
				# print ("Host to profile: " + hostName + ": UPDATED ")
				#===============================================================

			elif currentHost.shouldBeProfiled():
				if currentHost.lrms == None:
					currentHost.lrms = foundLrms
				hostsToProfile.append(currentHost)
				#===============================================================
				# print ("Host to profile: " + hostName + ": SHOULD BE PROFILED ")
				#===============================================================

				
		#pprofiling of new sites		
		hostProfilingTasks = [ExecutionManager.createHostProfilingTask(host) 
							for host in hostsToProfile
							for i in range(base.profilingTasksPerHost)]


		siteTasks = []
		for task in hostProfilingTasks:
			found=False
			for gridTask in infrastructureTasks:
				if gridTask.host.hostname == task.host.hostname:
					found=True
					break
			if not found:
				siteTasks.append(task)
				


		print ("	...Executing pilot tasks")
		pilotTasks = self.createPilotTasks(infrastructureTasks)
		for task in pilotTasks:
			siteTasks.append(task)
		

		return siteTasks
		

		
	def priorize(self, 	pilotList):
		pilotList.sort(key= lambda x: x.host.whetstones, reverse=True)