
'''
Created on Aug 31, 2012

@author: u5682
'''
from sqlalchemy import *
from DBDesign import DBDesign
from Host import Host
from Controller import Controller
import UserInterface
import ExecutionManager
import ExecutionStats
import Infrastructure, PilotInfrastructure, NewPilotInfrastructure
import RefreshCertificates
import base
import shutil

import sys
from datetime import datetime, timedelta

	
if __name__ == '__main__':

	print ("Updating date of the last failure")
	print ("Setting an old one, so every host can be employed again")
	print (".....")


	hostList = base.Session.query(Host).all()

	
	oldDate = datetime.now() - timedelta(weeks=2)
	
	for host in hostList:
		print ("Updating host " + host.hostname)
		
		if host.hostname =="":
			print ("Deleteing host, no name found")
			base.Session.delete(host)
		if host.type == "fakehosts":
			print ("Deleting fake host")
			base.Session.delete(host)
		if host.lrms =="jobmanager-pilot":
			print ("Removing host, it is a pilot.")
			base.Session.delete(host)
		if host.lrms =="NULL":
			print ("Removing host, unknown LRMS.")
			base.Session.delete(host)

		host.lastFailedProfiling = oldDate
		host.lastFailedTask =oldDate
		
	base.Session.commit()
	
	print ("")
	print ("Hosts have been updated")