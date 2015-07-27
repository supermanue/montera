'''
Created on Aug 24, 2012

@author: u5682
'''
from GridTask import GridTask
from Host import Host
from FakeHost import FakeHost

from Pilot import Pilot
import ExecutionManager
from Infrastructure import Infrastructure
from PilotResource import PilotResource
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


class NewPilotInfrastructure(Infrastructure):
	'''
	classdocs
	'''


	def __init__(self, *args, **kwargs):
		super(NewPilotInfrastructure, self).__init__()
		
		print ("Initializingi newPilot infrastructure. 0 pilots loaded")
		self.pilots = []
	
		#HOST: pilots corriendo, que pueden ejecutar trabajos. Se llaman pilot_X 
		#SITE: sitio remoto donde se ejecutan los pilots, 
		#Resource: pilot o site. 
	
	
	def getHost(self, hostname):
		foundHost = None
		for host in self.hosts:
			if host.hostname.strip().lower() == hostname:
				return host 
			
		for pilot in self.pilots:
			if pilot.hostname.strip().lower() == hostname:
				return pilot 
			
		return None
	

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
			hostname = resource.getElementsByTagName("HOSTNAME")[0].firstChild.data.strip().lower() #TODO: remove "unicode" from TEXT

			try:	
				foundLrms = resource.getElementsByTagName("LRMS_NAME")[0].firstChild.data.strip().lower()
			except:
				print ("Could not obtain resource LRMS, skipping it")
				continue
			
			if foundLrms != "jobmanager-pilot":
				continue
		
			#ahora son todos pilots
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
		
								
	
	#devuelve una lista de los hosts que sabemos que funcionan	
	#AQUI SOLO DEVUELVO PILOTS		
	def getGoodHosts(self):
		goddHosts=[]
		for pilot in self.pilots:
			if pilot.getWhetstones() > 0:
				goddHosts.append(pilot)
		return goddHosts
	
	

	def load(self):	
		print ("LOAD - pilotInfrastructures")			
		#SITES
		pastHosts = Session.query(Host).filter(Host.type=="hosts").all()
		presentHosts = []
		
		#PILOTS
		pastPilots = Session.query(Pilot).all()
		print ("Deleting all past pilots from database")
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
			print("Error when parsing host information file, employing information from past executions")	
			self.hosts = pastHosts
			self.pilots = []
			return
			
		
		#the hosts that we will first employ are the resulting ones
		self.pilots = presentPilots
		self.hosts = presentHosts
		
		
		
		
	def showHosts(self):
		
		print ("...........")
		print ("Showing host list")

		total = len(self.hosts)
		print ("Total number of Hosts: " + str(total))
		
		print ("")
		print ("Hosts: ")
		for site in self.hosts:
			print (site.hostname)

		print ("")
		print ("Pilots: ")
		for pilot in self.pilots:
			print (pilot.hostname + ", " + str(pilot.getWhetstones()) + " whet")
			
			
						
		print ("---") 
		
	
	
	
	#TODO: Este nombre es una mierda y no refleja bien su función. Hay que cambiarlo
	def createInfrastructureTasks(self, infrastructureTasks):	
		
		print ("-------------------")
		print ("-------------------")

		print ("createInfrastructureTasks- NewPilotInfrastructure")

	#	self.showHosts()
					
		hostList = obtainGWResources()
		
		hostsToProfile = []

		print ("Analyzing resources ")
		for hostInfo in hostList:
			hostName = hostInfo.getElementsByTagName("HOSTNAME")[0].firstChild.data.strip().lower() #TODO: remove "unicode" from TEXT
			whetstones=0

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
				
				username = os.getenv("USER")
				genericStringArgs = hostInfo.getElementsByTagName("GENERIC_VAR_STR")
				for node in genericStringArgs:
					if node.attributes['NAME'].value =="PILOT_REAL_HOSTNAME":
						workerNode = node.attributes['VALUE'].value.strip().lower()
					if node.attributes['NAME'].value =="PILOT_REAL_RESOURCE":
						site = node.attributes['VALUE'].value.strip().lower()
				
				genericIntArgs = hostInfo.getElementsByTagName("GENERIC_VAR_INT")
				for node in genericIntArgs:
					if node.attributes['NAME'].value =="PILOT_" + username + "_VAR_5":
						whetstones = int(node.attributes['VALUE'].value.strip().lower())
						if whetstones > 65534: 
							whetstones = 0
				# 	whetstones = 0
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
						print ("	PilotResource was not found, creating a new one")
						pilotResource = PilotResource(site, workerNode)
					print ("	Creating a new Pilot in NewPilotInfrastructure.createInfrastructureTasks")
					newHost = Pilot(hostName, arch=foundArch, cpuMHz = foundCpuMHz, pilotResource = pilotResource, whetstones = whetstones)
					self.pilots.append(newHost)
					Session.add(newHost)

				else:
					print ("	Creating a new Host in NewPilotInfrastructure.createInfrastructureTasks")
					newHost = Host(hostName, arch=foundArch, cpuMHz = foundCpuMHz, lrms=foundLrms)
					self.hosts.append(newHost)
					Session.add(newHost)

				#ESTO ES PARA HACER EL PROFILING DE LOS PILOT SI NO HAN PUBLICADO LOS WHETSTONES, SI NO NO HACE FALTA	
				#===============================================================
				# if whetstones == 0 or whetstones > 65534: 
				# 	whetstones = 0
				# 	print ("	Host to profile: " + hostName + ": whetstone value not initialized ")
				# 	hostsToProfile.append(newHost)
				# 	#store new host on databae (faiulre resistance
				# 	Session.add(newHost)
				#===============================================================
				
			#if information has changed, update host information
			elif (currentHost.getWhetstones() != whetstones):
				#va con un set porque es una operación más complicada, así que está encapsulada en esta funcion
				currentHost.setWhetstones(whetstones)	
				Session.add(currentHost)
				print ("Host: " + hostName + " UPDATED, new whetstones=" + str(whetstones))

			elif currentHost.lrms == None:
				currentHost.lrms = foundLrms


		#pprofiling of new sites		
		hostProfilingTasks = [ExecutionManager.createHostProfilingTask(host) 
							for host in hostsToProfile
							for i in range(base.profilingTasksPerHost)]
		
		

	
		#estamos asumiento que todos los pilots publican la variable esa con su 
		#rendimiento, con lo que no hay que hacer el profiling de nada. 		
				
		#AHORA, EN ESA NUEVA APROXIMACION, QUEREMOS TENER UNOS CUANTO SBENCHMARKS PARA IR ARRANCANDO PILOTS 
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
		fakeHostProfilingTasks = [ExecutionManager.createFakeHostProfilingTask(emptyHost) 
					for i in range(fakeTasksToCreate)]

		hostProfilingTasks+=fakeHostProfilingTasks
		
		
		return hostProfilingTasks
