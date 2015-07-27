'''
Created on Aug 31, 2012

@author: u5682
'''
from sqlalchemy import *

class DBDesign(object):
	'''
	classdocs
	'''


	def __init__(self):
		'''
		Constructor
		'''
		
	def HostDBDesign(self,metadata):
		return Table('hosts', metadata, 
					Column('id', Integer, primary_key=True),
					Column('hostname', String(256)),
					Column('currentSlotCount', Integer),
					Column('maxSlotCount', Integer),
					Column('arch', String(256)),
					Column('cpuMHz', Integer),
					Column('maxCPUTime', Integer),
					Column('lrms', String(256)),
					Column('whetstones', Float),
					Column('whetstonesTypicalDeviation', Float),
					Column('queueTime', Float),
					Column('queueTimeTypicalDeviation', Float),
					Column('bandwidth', Float),
					Column('failedProfilings', Integer),
					Column('lastFailedProfiling', DateTime),
					Column('successfulExecutions', Integer),
					Column('failedExecutions', Integer),
					Column('lastFailedExecution', DateTime),
					Column('type', String(50))
					 )
	
	def PilotDBDesign(self, metadata):
		return Table('pilots', metadata, 
					Column('id', Integer, primary_key=True),
					Column('pilotResourceID', Integer)
					)
		
	def PilotResourceDBDesign(self, metadata):
		return Table ('pilotResources', metadata, 
					Column('id', Integer, primary_key=True),
					Column('site', String(256)),
					Column('workerNode', String(256)),
					Column('whetstones', Float),
					Column('bandwidth', Float),
					Column('successfulExecutions', Integer),
					Column('failedProfilings', Integer)
					)


	def AppProfileDBDesign(self,metadata):
		return Table('appProfiles', metadata, 
					Column('id', Integer, primary_key=True),
					Column('applicationName', String(256), unique=True),
					Column('constantEffort', Float),
					Column('sampleEffort', Float),
					Column('numProfilings', Integer))
		
	def ApplicationDesign(self, metadata):
		return Table('applications', metadata,
					Column('id', Integer, primary_key=True),
					Column('name', String(256)),
					Column('desiredSamples', Integer),
					Column('remainingSamples', Integer),
					Column('maxSupportedSamples', Integer),
					Column('maxProfilingTime', Integer),
					Column('preProcessScript', String(256)),
					Column('postProcessScript', String(256)),
					Column('inputFiles', String(256)),
					Column('outputFiles', String(256)),
					Column('requirements', String(256)),
					Column('environment', String(256)),
					Column('workingDirectory', String(256)),
					Column('appProfileID', Integer),
					Column('finished', Integer),
					Column('schedulingAlgorithm', String(256)),
					Column('remoteMontera', String(256))
					)
		
	def GridTaskDBDesign(self, metadata):
		return Table('gridTasks', metadata, 
					Column('id', Integer, primary_key=True),
					Column('type', String(256)),
					Column('gwID', String(256)),
					Column('status', String(256)),
					Column('minSamples', Integer),
					Column('maxSamples', Integer),
					Column('realSamples', Integer),
					Column('hostID', Integer),
					Column('applicationID', Integer),
					Column("creationDate", DateTime),
					Column("executionStartDate", DateTime),
					Column("endDate", DateTime)					
					)		
	#===========================================================================
	#			
	# def parameterDBDesign(self, metadata):
	#	return Table('parameters', metadata, 
	#				Column('id', Integer, primary_key=True),
	#				Column('parameter', String(256)),
	#				Column('status', String(256)),
	#				Column("creationDate", DateTime),
	#				Column("executionStartDate", DateTime),
	#				Column("endDate", DateTime)					
	#				)		
	#===========================================================================