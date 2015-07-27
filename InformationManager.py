'''
Created on Aug 29, 2012

@author: u5682
'''
import os
from DyTSS import DyTSS

import base

import subprocess as sub
import xml.dom.minidom
from xml.dom.minidom import Node



class InformationManager(object):
	'''
	classdocs
	'''


def readTotalActiveTime(gwID):
	logFilePath = os.environ['GW_LOCATION']+ "/var/" + gwID + "/job.log"

	try:
		logFile = open(logFilePath)
	except:
		print ("Active time from task " + gwID + " could not be found")
		return -1
	
	searchFor = "Active time"
	activeTime = -1
	try:
		for line in logFile:
			if line.count(searchFor) > 0:
				print ("readTotalActiveTime, line read: " + line)
				auxLine = line.split(":")
				activeTime = int(auxLine[-1].strip())
	except:
		print ("ERROR when reading active time from gridway task: " + gwID)		
	return activeTime
	




def readQueueTime(gwID):
	logFilePath = os.environ['GW_LOCATION']+ "/var/" + gwID + "/job.log"

	try:
		logFile = open(logFilePath)
	except:
		print ("Queue time from task " + gwID + " could not be found")
		return -1
	 
	searchFor = "Suspension time"
	suspensionTime = -1
	for line in logFile:
		if line.count(searchFor) > 0:
			auxLine = line.split(":")
			suspensionTime = int(auxLine[-1].strip())
	if suspensionTime == -1:
		print ("ERROR when reading queue time from gridway task: " + gwID)		
	return suspensionTime			

def loadSchedulingAlgorithm(name, initialized = False, debug = False):
	if name =="dytss":
		myAlgorithm = DyTSS(initialized, debug)
		return myAlgorithm
	else:
		return None
	
	
def readMaxRunningTasks():
	confFilePath = os.environ['GW_LOCATION']+ "/etc/sched.conf"

	logFile = open(confFilePath)
	
	searchFor = "MAX_RUNNING_USER"
	maxRunningTasks = -1
	for line in logFile:
		if line.count(searchFor) > 0:
			if line.startswith("#"):
				continue
			auxLine = line.split("=")
			maxRunningTasks = int(auxLine[-1].strip())
	if maxRunningTasks == -1:
		print ("ERROR when reading number of running tasks allowed: ")	
		
	if maxRunningTasks == 0:
		maxRunningTasks = base.maxRunningTasks
	print ("Maximum number of running tasks allowed: " + str(maxRunningTasks))	
	return maxRunningTasks
	
	

def obtainGWResources():
	#create XML with all the host info from GridWay
	gwResourcePath = "/tmp/gwhostInfo"
	gwResourcePathOld = "/tmp/gwhostInfoOld"
		
	command = "gwhost -x" 
	
	f = open(gwResourcePath, 'wb')
		
	p = sub.Popen(command, shell = True, stdout=f)
	p.wait()
	res = p.communicate()[0]
	
	if p.returncode:
		msg = 'Error in "{0}". Exit code: {1}. Output: {2}'
		msg = msg.format(command, p.returncode, res)
	# 	avoid crash
	#	raise msg
	
	#load XML to memory,  and extract data from hosts
	try:
		#fileName = open("/tmp/gwhostInfo")
		aux = xml.dom.minidom
		doc = aux.parse(gwResourcePath)
		#fileName.close()
		hostList = doc.getElementsByTagName('gwhost')[0].getElementsByTagName("HOST")
		os.rename(gwResourcePath, gwResourcePathOld)

	except:
		print ("InfrastructureManager:obtainGWResources")
		print 	("	... ha fallado la obtencion de hosts, empleando fichero antiguo")
		
		aux = xml.dom.minidom
		doc = aux.parse(gwResourcePathOld)
		#fileName.close()
		hostList = doc.getElementsByTagName('gwhost')[0].getElementsByTagName("HOST")

	
	return hostList


def checkForValidCertificates():
	
	p = sub.Popen("voms-proxy-info -actimeleft", shell = True, stdout=sub.PIPE)
	p.wait()
	timeLeft = p.communicate()[0]
	
	# If error, raise exception
	try: 
		if p.returncode:
			print (timeLeft)
			print ("ERROR")
			print ("Could not obtain certificate time, so we'll asume it is still valid")
			return True
	except:
		print ("just for the lulz LOL, an additional error check")
		return True
	
	for line in timeLeft.splitlines():
		time = int(line.strip())
		if time > 0:
			return True
		else:
			return False
			
