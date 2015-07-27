'''
Created on Jan 11, 2013

@author: u5682
'''
import InformationManager
import ExecutionManager
import UserInterface

from Infrastructure import Infrastructure
from Application import Application
from GridTask import GridTask
from DyTSS import DyTSS

from time import sleep
from datetime import datetime
import sys
from base import Session

from sqlalchemy import or_
import subprocess as sub
import xml.dom.minidom
from xml.dom.minidom import Node
import os, sys


def runProcess(command):	
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


def processGWHosts():
	runProcess("gwhost -f  > "+ gwhostLocation)
	fileToRead = open(gwhostLocation, 'r')
	
	hosts=[]
	for line in fileToRead.readlines():
		if line.count("HOSTNAME") >0:
			hostname = line.split("=")[1].strip()
			hosts.append(hostname)
			continue
	return hosts
	
	
def refreshCertificates( myInfrastructure = None):
	gwhostLocation = "/tmp/gwHosts"
	print ("OBTAINING AVAILABLE HOSTS")
	
	if myInfrastructure == None:
		myInfrastructure = Infrastructure()

	myInfrastructure.load()
	#hostList = myInfrastructure.getGoodHosts()


	print ("UPDATING CERTIFICATE IN REMOTE HOSTS")
	renewedHostList = []
	for gwHost in myInfrastructure.hosts:		
		print ("-----")
		print ("Host: " + gwHost.hostname)
		print ("	host found in past executions")
		command = "glite-ce-proxy-renew -e " + gwHost.hostname + " gw01.ciemat.es"
		print ("	" + command)
		
		
		try:
			result = runProcess(command)
			print (result)
			print ("	parece que ruló")
			renewedHostList.append(gwHost)
		except:
			print ("parece que falló")

	print ("")
	print("RENEWED CERTIFICATES")
	for host in renewedHostList:
		print (host.hostname)

	
if __name__ == '__main__':
	refreshCertificates()
