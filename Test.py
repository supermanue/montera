'''
Created on Jul 9, 2013

@author: u5682
'''
'''
Created on Aug 31, 2012

@author: u5682
'''
from sqlalchemy import *

from base import Session
from base import Base, Session, engine
import base

from datetime import datetime

from DBDesign import DBDesign
from Host import Host
from Controller import Controller
import UserInterface
import ExecutionManager
import ExecutionStats
import Infrastructure
import shutil

import sys
from Application import Application
from GridTask import GridTask
import UserInterface

if __name__ == '__main__':
	requirementsFile = "/soft/users/home/u5682/nfs/workspace/montera/FLUKA/fluka_example.mt"

	print("Starting connection with database")
			
	metadata = MetaData()
	
	myDBDesign = DBDesign()
	
	hostDB = myDBDesign.HostDBDesign(metadata)
	applicationDB = myDBDesign.ApplicationDesign(metadata)
	appProfileDB = myDBDesign.AppProfileDBDesign(metadata)
	gridTaskDesignDB = myDBDesign.GridTaskDBDesign(metadata)
	parameterDesignDB = myDBDesign.parameterDBDesign(metadata)

	metadata.create_all(base.engine)
	
	print("Database is correct")
	print("")
	
	
	myInfrastructure = Infrastructure.Infrastructure('lrms')
	myInfrastructure.createHostProfilingTasks()
	

	Session.add(myInfrastructure)
	Session.commit()
