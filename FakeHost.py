'''
Created on Oct 11, 2013

@author: u5682
'''


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
from Host import Host

import math
import Bessel

from sqlalchemy import *
import base
from base import Base
from sqlalchemy.orm import relationship, backref

import shutil

class FakeHost(Host):	

	__mapper_args__ = {
			'polymorphic_identity': 'fakehosts'		
					}
	
	
	
	def __init__(self, *args, **kwargs):
		'''
		Constructor
		'''
		self.hostname = ""
		
	
	def getWhetstones(self):
		return 0
	def getBandwidth(self):
		return 0
	def getQueueTime(self):
		return 0
	
	def updateInfoAfterFailedProfiling(self):
		print ("	Reported failure on a Fake host, it is not a problem")
		return 0