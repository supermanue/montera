'''
Created on Aug 31, 2012

@author: u5682
'''

from sqlalchemy import exc
from sqlalchemy import event
from sqlalchemy.pool import Pool


from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy import create_engine
import os
import sys

@event.listens_for(Pool, "checkout")
def ping_connection(dbapi_connection, connection_record, connection_proxy):
	cursor = dbapi_connection.cursor()
	try:
		cursor.execute("SELECT 1")
	except:
		# optional - dispose the whole pool
		# instead of invalidating one at a time
		# connection_proxy._pool.dispose()

		# raise DisconnectionError - pool will try
		# connecting again up to three times before raising.
		print ("Raising a disconnection error")
		raise exc.DisconnectionError()
	cursor.close()


Base = declarative_base()


monteraLocation = os.path.dirname(__file__)

executionFiles = monteraLocation + "/executionFiles/"
profilingFiles = monteraLocation+ "/profilingFiles/"
resultFiles = monteraLocation + "/resultFiles/"
pilotFiles = monteraLocation + "/pilotFiles/"


#CONFIGURATION FILE
configurationFiles = monteraLocation +"/configFiles"
hostname = os.getenv('HOSTNAME')
configurationFile = os.path.join(configurationFiles, hostname)


#default values
infrastructureType = None
lrms = None
initRetryNumber=1
maxRunningTasks = 100
desiredFakeTasks = 50
profilingTasksPerHost = 2
maxPilotsPerSite = 10

maxExecutionTime=24*3600 #seconds
maxOverhead = 30 #percent
spareTasks = 20 #percent




for line in open(configurationFile):
	arguments = [x.strip() for x in line.split("=")] #take both parts of the asignment and remove spaces and so

	if arguments[0] == "tmpExecutionFolder":
		tmpExecutionFolder = arguments[1]
	elif arguments[0] == "dbType":
		dbType = arguments[1]
	elif arguments[0] == "dbUser":
		dbUser = arguments[1]
	elif arguments[0] == "dbPassword":
		dbPassword = arguments[1]
	elif arguments[0] == "lrms":
		lrms = arguments[1]
	elif arguments[0] == "infrastructureType":
		infrastructureType = arguments[1]
	elif arguments[0] == "initRetryNumber":
		initRetryNumber = int(arguments[1])
	elif arguments[0] == "maxRunningTasks":
		maxRunningTasks = int(arguments[1])
	elif arguments[0] == "maxPilotsPerSite":
		maxPilotsPerSite = int(arguments[1])
	elif arguments[0] == "maxExecutionTime":
		maxExecutionTime = int(arguments[1])
	elif arguments[0] == "maxOverhead":
		maxOverhead = int(arguments[1])
	elif arguments[0] == "spareTasks":
		spareTasks = int(arguments[1])	

if dbType == "mysql":
	engine = create_engine('mysql://'+dbUser+":" + dbPassword + '@localhost/Montera', echo=False, echo_pool=True)
else:
	print ("Now, only mysql is supported. Sorry dude")
	exit(-1)
#-------------------------------------------------------------------------- try:
	# engine = create_engine('mysql://root:root@localhost/Montera', echo=False, echo_pool=True)
	#--------------------------------- print ("Database engine selected: MySQL")
#----------------------------------------------------------------------- except:
	#---------------------------------------------------------------------- try:
		# engine = create_engine('mysql://root:monteraPass2013@localhost/Montera', echo=False, echo_pool=True)
		#------------- print ("Database engine selected: MySQL, different pass")
	#------------------------------------------------------------------- except:
		#------------------- print ("No engine available, crashing in 3,2,1...")
		#---------------------------------------------------------- sys.exit(-1)

Session = scoped_session(sessionmaker(bind=engine))
