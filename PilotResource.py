'''
Created on Sep 2, 2013

@author: u5682
'''

from sqlalchemy import *
import base
from base import Base
from sqlalchemy.orm import relationship, backref
import Bessel


class PilotResource(Base):
	'''
	classdocs
	'''
	__tablename__ = 'pilotResources'
	id = Column(Integer, primary_key=True)
	workerNode = Column(String)
	site = Column(String)
	whetstones = Column(Float)
	bandwidth = Column(Float)
	successfulExecutions = Column(Integer)
	failedProfilings = Column(Integer)

	'''
	classdocs
	'''


	def __init__(self, site, workerNode, whetstones = 0, bandwidth = 0, succesfulExecutions = 0):
		'''
		Constructor
		'''
		self.workerNode = workerNode
		self.site = site
		self.whetstones = whetstones
		self.bandwidth = bandwidth
		self.successfulExecutions = succesfulExecutions
		self.failedProfilings = 0
		
	
	def updateInfoFromSuccessFulExecution(self, whetstones, queueTime, bandwidth):
		if self.successfulExecutions == 0:
			self.whetstones = whetstones			
			if bandwidth > 0:
				self.bandwidth = bandwidth
		else:
			
			oldWhetstones = self.whetstones
			newWhetstones = Bessel.calculateAverage(oldWhetstones, self.successfulExecutions, whetstones)	
			self.whetstones = newWhetstones
			
			if bandwidth > 0:
				oldBandwidth = self.bandwidth
				newBandwidth = Bessel.calculateAverage(oldBandwidth, self.successfulExecutions, bandwidth)
				self.bandwidth = newBandwidth
			
		self.successfulExecutions +=1
		self.failedProfilings -=1