'''
Created on 24/04/2013

@author: supermanue
'''


from __future__ import division
import sys, os
from datetime import datetime

import xml.dom.minidom
from xml.dom.minidom import Node
import subprocess as sub

from base import Session
from Parameter import Parameter
from base import monteraLocation
class Analyzer(object):
	
	numTasks = 0
	parametricJobFile = None
	
	def __init__(self, parametricJobFile):
		'''
		Constructor
		'''
		self.parametricJobFile = parametricJobFile
		self.analyzeParametricJobfile()
	

	def runProcess(self,command):   
			# Start subprocess
			print ("Executing command: " + command)
			p = sub.Popen(command + ' 2>&1', shell = True, stdout=sub.PIPE)
			# Wait for it to finish
			p.wait()
			# Get output
			res = p.communicate()[0]
	
			# If error, raise exception
			if p.returncode:
					raise 'Error in "{0}". Exit code: {1}. Output: {2}'
	
			# Else, return output
			return res
	
	
	
	
	def analyzeParametricJobfile(self):
		
		tmpFile = "/tmp/tmpParameterFile"
		process = monteraLocation + '/templateManager -c ' +  self.parametricJobFile + " > " + tmpFile
		parameters = self.runProcess(process)
		numTasks = 0
		
		for line in open(tmpFile, 'r').readlines():
			print ("Parameter: " + line)
			myParameter = Parameter(line) 
			Session.add(myParameter)
			numTasks+=1
			
		Session.commit()		