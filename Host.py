'''
Created on Aug 24, 2012

@author: u5682
'''


from datetime import datetime, timedelta
import xml.dom.minidom
from xml.dom.minidom import Node
from DRMAA import *
import sys, os

import GridTask


import math
import Bessel

from sqlalchemy import *
import base
from base import Base
from sqlalchemy.orm import relationship, backref

import shutil


class Host(Base):
	'''
	classdocs
	'''
	__tablename__ = 'hosts'
	id = Column(Integer, primary_key=True)
	hostname = Column(String)
	currentSlotCount = Column(Integer)
	maxSlotCount = Column(Integer)
	arch = Column(String)
	cpuMHz = Column(Integer)
	maxCPUTime = Column(Integer)
	lrms = Column(String)
	whetstones = Column(Float)
	whetstonesTypicalDeviation = Column(Float)
	queueTime = Column(Float)
	queueTimeTypicalDeviation = Column(Float)
	bandwidth = Column(Float)
	failedProfilings = Column(Integer)
	lastFailedProfiling= Column(DateTime)
	successfulExecutions = Column(Integer)
	failedExecutions = Column(Integer)
	lastFailedExecution = Column(DateTime)
	type = Column(String(50))
	
	
	__mapper_args__ = {
        'polymorphic_identity':'hosts',
        'polymorphic_on':type
    }
	
	
	maxSlotCountThisTime = 1

	#gridTasks = relationship("GridTask", backref = "host")
	
	def __init__(self, hostname, 
				currentSlotCount = 0,
				maxSlotCount = 1,
				maxSlotCountThisTime = 1,
				arch = "",
				cpuMHz = 0,
				maxCPUTime = 0,
				lrms = None,
				whetstones = 0,
				whetstonesTypicalDeviation = 0,
				queueTime = 0,
				queueTimeTypicalDeviation = 0,
				bandwidth = 0,
				failedProfilings = 0,
				lastFailedProfiling = None,
				successfulExecutions = 0,
				failedExecutions = 0,
				lastFailedExecution =None
				
				):
		'''
		Constructor
		'''
		self.hostname = hostname.strip().lower()
		
		#slot info
		self.currentSlotCount = currentSlotCount
		self.maxSlotCount = maxSlotCount
		self.maxSlotCountThisTime = maxSlotCountThisTime
		
		#architecture
		self.arch = arch
		self.cpuMHz = cpuMHz
		self.maxCPUTime = maxCPUTime
		self.lrms = lrms
		#performance
		self.whetstones = whetstones
		self.whetstonesTypicalDeviation = whetstonesTypicalDeviation
		self.queueTime = queueTime
		self.queueTimeTypicalDeviation = queueTimeTypicalDeviation
		self.bandwidth = bandwidth
		
		self.failedProfilings = failedProfilings
		self.lastFailedProfiling = lastFailedProfiling
		
		#executions
		self.successfulExecutions = successfulExecutions
		self.failedExecutions = failedExecutions
		self.lastFailedExecution = lastFailedExecution

		#esto es raro
		if (self.lastFailedProfiling == None):
			self.lastFailedProfiling = datetime.now()
		if (self.lastFailedExecution == None):
			self.lastFailedExecution = datetime.now()	
		
		print ("	New Host created with hostname: " + self.hostname)
		
		try:
			equalHosts = base.Session.query(Host).filter(Host.hostname==self.hostname).count()
			if equalHosts > 0:
				print ("	A host with same hostname already existed, big problemo!!!")
		except:
				print ("	Assuming there are no hosts with the same name, but I cannot be sure")

		
	
	def __repr__ (self):
		print (self.hostname + ". succesful exec: " + str(self.successfulExecutions) + ", failed profilings: " + str(self.failedProfilings))
# 		print ("	whetstones, queue time, bandwidth " + str(self.whetstones) + ", " + str(self.queueTime) + ",	" + str(self.bandwidth))
	
	#this method updates the information about a host
	def updateInfoFromSuccessFulExecution(self, whetstones, queueTime, bandwidth):

		if self.successfulExecutions == 0:
			self.whetstones = whetstones
			self.whetstonesTypicalDeviation = 0
			
			self.queueTime = queueTime
			self.queueTimeTypicalDeviation= 0
			
			if bandwidth > 0:
				self.bandwidth = bandwidth
		else:
			
			oldWhetstones = self.whetstones
			newWhetstones = Bessel.calculateAverage(oldWhetstones, self.successfulExecutions, whetstones)
	
			oldWhetstonesDeviation = self.whetstonesTypicalDeviation
			newWhetstonesDeviation = Bessel.calculateTypicalDeviation(oldWhetstones, oldWhetstonesDeviation, self.successfulExecutions, whetstones)
	
			self.whetstones = newWhetstones
			self.whetstonesTypicalDeviation = newWhetstonesDeviation
			
			oldQueueTime = self.queueTime
			newQueueTime = Bessel.calculateAverage(oldQueueTime, self.successfulExecutions, queueTime)
	
			oldQueueTimeDeviation = self.queueTimeTypicalDeviation
			newQueueTimeDeviation = Bessel.calculateTypicalDeviation(oldQueueTime, oldQueueTimeDeviation, self.successfulExecutions, queueTime)
	
			self.queueTime = newQueueTime
			self.queueTimeTypicalDeviation = newQueueTimeDeviation
			
			if bandwidth > 0:
				oldBandwidth = self.bandwidth
				newBandwidth = Bessel.calculateAverage(oldBandwidth, self.successfulExecutions, bandwidth)
				self.bandwidth = newBandwidth
			
		self.successfulExecutions +=1
		
		#si ha funcionado una tarea ponemos el contador de fallos a 0, para que el sitio no sea baneado
		#(y si han fucionado más de una, pues negativo)
		self.failedProfilings =max (0, self.failedProfilings -1)
		print ("	failed profilings: " + str(self.failedProfilings) + " on host " + self.hostname)

	
	
	
		#determine if a given host should be profiled.
		#it is needed it less than 3 profilings have been performed, or last one was more than one week ago
	def shouldBeProfiled(self):
				
		lastWeek = datetime.now()- timedelta(days=7)

		yesterday = datetime.now()- timedelta(days=1)
		
		if self.successfulExecutions == 0 and self.failedProfilings <3:
			#===================================================================
			# print ("host " + self.hostname + ": should be profiled again")
			# print ("	failed profilings: " + str(self.failedProfilings))
			#===================================================================
			return True
		if self.successfulExecutions == 0 and self.failedProfilings >=3 and self.lastFailedProfiling < yesterday:
			#===================================================================
			# print ("host " + self.hostname + ": should be profiled again")
			# print("	last profiling was more than a week ago, on " + str(self.lastFailedProfiling))
			#===================================================================

			return True
		
		return False

	def updateInfoAfterFailedProfiling(self):
		self.failedProfilings += 1
		self.lastFailedProfiling = datetime.now()
		print ("	failed profilings: " + str(self.failedProfilings) + " on host " + self.hostname)

		
	def updateInfoAfterFailedExecution(self):
		self.failedExecutions += 1
		self.failedProfilings += 1

		self.lastFailedExecution = datetime.now()
		self.lastFailedProfiling = datetime.now()
		print ("	failed profilings: " + str(self.failedProfilings) + " on host " + self.hostname)

	def exportInfoToFile(self, path):
		result=""
		result+="<hostname>" + self.hostname + "</hostname>\n"
		result+="<arch>" + self.arch + "</arch>\n"
		result+="<cpu_mhz>" + str(self.cpuMHz) + "</cpu_mhz>\n"
		result+="<max_cpu_time>" + str(self.maxCPUTime) + "</max_cpu_time>\n"
		result+="<whetstones>" + str(self.whetstones) + "</whetstones>\n"

		
		f = open(path, 'w')
		f.write(result)
		f.close()
		
	def getWhetstones(self):
		return self.whetstones
	
	def setWhetstones(self, whetstones):
		self.whetstones = whetstones
		
	def getBandwidth(self):
		return self.bandwidth
	def getQueueTime(self):
		return self.queueTime
		