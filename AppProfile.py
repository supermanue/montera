'''
Created on Aug 28, 2012

@author: u5682
'''
from DRMAA import *
import sys, os, shutil
from GridTask  import GridTask
import math
import Bessel

from sqlalchemy import *
from base import Base, profilingFiles
from sqlalchemy.orm import relationship, backref


class AppProfile(Base):
	
	__tablename__ = 'appProfiles'
	id = Column(Integer, primary_key=True)
	applicationName = Column(String, unique=True)
	constantEffort = Column(Float)
	sampleEffort = Column(Float)
	numProfilings = Column(Integer)

#NOTA: también esta "application" como parametro pero eso se craga desde application con un backref.
#es bastante raro eso, lo mismo hay alguna solución mejor o algo... no se

	def __init__(self, applicationName, application = None, constantEffort = -1, sampleEffort = -1, numProfilings = 0):
		self.applicationName = applicationName
		self.application = application
		self.constantEffort = constantEffort
		self.sampleEffort = sampleEffort
		self.numProfilings = numProfilings
			
	#===========================================================================
	#		
	# def __init__(self, applicationName):
	#	self.applicationName = applicationName
	#===========================================================================

		
		
	def updateInfoAfterProfiling(self, constantEffort, sampleEffort):
		
		if (self.numProfilings == 0):
			self.constantEffort = constantEffort
			self.sampleEffort = sampleEffort
			
			
		else:
			oldConstantEffort = self.constantEffort
			newConstantEffort = Bessel.calculateAverage(oldConstantEffort, self.numProfilings, constantEffort)
			
			
			oldSampleEffort = self.sampleEffort
			newSampleEffort = Bessel.calculateAverage(oldSampleEffort, self.numProfilings, sampleEffort)
				
			self.constantEffort = newConstantEffort
			self.sampleEffort = newSampleEffort
			
		self.numProfilings += 1

		
		
