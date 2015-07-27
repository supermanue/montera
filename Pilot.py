'''
Created on Aug 31, 2013

@author: u5682
'''
from Host import Host
from sqlalchemy import *
from sqlalchemy.orm import relationship, backref


class Pilot(Host):
	__tablename__ = 'pilots'
	id = Column(Integer, ForeignKey('hosts.id'), primary_key=True)
	pilotResourceID = Column(Integer, ForeignKey('pilotResources.id'))
	pilotResource = relationship("PilotResource", cascade_backrefs=False)

	
	__mapper_args__ = {
			'polymorphic_identity': 'pilots'		
					}
	
	'''
	classdocs
	'''


	def __init__(self, *args, **kwargs):
		super(Pilot, self).__init__(*args)

		self.pilotResource = kwargs['pilotResource']
		
		self.lrms="jobmanager-pilot"
		print ("	New Pilotf created with hostname: " + self.hostname)
		'''
		Constructor
		'''
	
	
	
	#this method updates the information about a host
	def updateInfoFromSuccessFulExecution(self, whetstones, queueTime, bandwidth):
		self.pilotResource.updateInfoFromSuccessFulExecution(whetstones, queueTime, bandwidth)


	def getWhetstones(self):
		if self.id == None or self.pilotResource.whetstones == "none" or int(self.pilotResource.whetstones) == 65535:
			return 0
		
		return self.pilotResource.whetstones
	def setWhetstones(self, whetstones):
		self.pilotResource.whetstones = whetstones	
	def getBandwidth(self):
		if self.id == None or self.pilotResource.bandwidth == "none":
			return 0
		return self.pilotResource.bandwidth
	def getQueueTime(self):
		return 0
	
	
	