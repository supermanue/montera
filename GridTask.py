'''
Created on Aug 24, 2012

@author: u5682
'''

#NOTA: lleva "template" porque se emplea para ejecutar cualquier cosa
#template es lo que se ejecutará en Grid, que hay que enviar y tal

from sqlalchemy import *
from base import Base
from sqlalchemy.orm import relationship, backref

from datetime import datetime
import Host

class GridTask(Base):
	'''
	classdocs
	'''
	__tablename__ = 'gridTasks'
	id = Column(Integer, primary_key=True)
	type = Column(String)
	gwID = Column(String)
	status = Column(String)
	minSamples  = Column(Integer)
	maxSamples  = Column(Integer)
	realSamples  = Column(Integer)
	hostID = Column(Integer, ForeignKey('hosts.id'))
	applicationID = Column(Integer, ForeignKey('applications.id'))
	
	creationDate = Column(DateTime)
	executionStartDate = Column(DateTime)
	endDate = Column(DateTime)

	host = relationship("Host", cascade_backrefs=False)
	application = relationship("Application", cascade_backrefs=False)



	def __init__(self, host, template, type, gwID = "-1", 
				status = "WAITING",
				minSamples = 0,
				maxSamples = 0,
				realSamples = 0, 
				application = None):
		'''
		Constructor
		'''
		self.host = host
		self.template = template
		self.type = type #TODO: ocumentar los types que hay
		self.gwID = gwID
		self.status = status
		
		self.minSamples = minSamples
		self.maxSamples = maxSamples
		self.realSamples = realSamples
		
		self.creationDate = datetime.now()
		self.application = application
		
		

	def duplicate(self, otherGridTask):
		self.host = otherGridTask.host
		self.template = otherGridTask.template
		self.type = otherGridTask.type #TODO: DEFINIT ESTO POR AQUI
		self.gwID = otherGridTask.gwID
		self.status = otherGridTask.status
		
		self.minSamples = otherGridTask.minSamples
		self.maxSamples = otherGridTask.maxSamples
		self.realSamples = otherGridTask.realSamples
		
		self.application = otherGridTask.application
		
		
	def expectedExecutionTime(self):
		fileSize = self.application.calculateInputFileSize()

		walltime = float(self.host.getQueueTime() + self.application.profile.constantEffort)
		walltime +=  float(self.host.getWhetstones())
		walltime +=	float(self.maxSamples * self.application.profile.sampleEffort / self.host.getWhetstones())
		
		if self.host.bandwidth > 100:
			walltime +=	fileSize / self.host.getBandwidth()
		return walltime
