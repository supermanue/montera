'''
Created on Aug 23, 2013

@author: u5682
'''

import base
from datetime import datetime, timedelta
import sys, os
from DBDesign import DBDesign
import pickle
import StringIO,tarfile

from sqlalchemy import *

from sqlalchemy.ext.serializer import loads, dumps


class dbManagement(object):

	def __init__(self):
		self.metadata = MetaData()
		self.myDBDesign = DBDesign()
	
		user = base.dbUser
		password = base.dbPassword
	
	
	def exportDB(self,path):
	
	
		print ("Cleaning output file")
		if os.path.exists(path):
			#os.remove(path)
			print ("in dbManagement.dbManagement I would be deleting " + path)
		exportFile = open(path, 'wr')

	#===========================================================================
	#	print ("Epxorting hosts...")
	#	tableToExport = self.myDBDesign.HostDBDesign(self.metadata)
	#	q = base.Session.query(tableToExport)
	#	serialized_data = dumps(q.all())
	#	exportFile.write(serialized_data)
	# 
	#	print ("Exporting profiles...")
	#	tableToExport = self.myDBDesign.AppProfileDBDesign(self.metadata)
	#	q = base.Session.query(tableToExport)
	#	serialized_data = dumps(q.all())
	#	exportFile.write(serialized_data)
	#			
	#	print ("Everything exported, exiting..")
	#	exportFile.close()
	#===========================================================================
		
		print "Exporting Database. This may take some time. Please wait ..."


		hostDB = self.myDBDesign.HostDBDesign(self.metadata)
		#applicationDB = self.myDBDesign.ApplicationDesign(self.metadata)
		appProfileDB = self.myDBDesign.AppProfileDBDesign(self.metadata)
		#gridTaskDesignDB = self.myDBDesign.GridTaskDBDesign(self.metadata)
		
		
		self.metadata.create_all(base.engine)
		tables = self.metadata.tables

		tar = tarfile.open(path,'w:bz2') 
		for tbl in tables:
			print "Exporting table %s ..." % tbl
			table_dump = dumps(base.engine.execute(tables[tbl].select()).fetchall())

			ti = tarfile.TarInfo(tbl)
			ti.size = len(table_dump)
			tar.addfile(ti, StringIO.StringIO(table_dump))

		print "Database exported! Exiting!"

		
		
	def importDB(self,path):
		
		#hostDB = self.myDBDesign.HostDBDesign(self.metadata)
		#applicationDB = self.myDBDesign.ApplicationDesign(self.metadata)
		appProfileDB = self.myDBDesign.AppProfileDBDesign(self.metadata)
		#gridTaskDesignDB = self.myDBDesign.GridTaskDBDesign(self.metadata)
	#===========================================================================
	# 
	#	print ("Cleaning previous state")
	#	hostDB.drop(base.engine)
	#	applicationDB.drop(base.engine)
	#	appProfileDB.drop(base.engine)
	#	gridTaskDesignDB.drop(base.engine)
	#	
	#===========================================================================

		print ("Creating empty tables")
		self.metadata.create_all(base.engine)

#===============================================================================
# 
#		print ("Reading data file")
# 
#		importFile = open(path, 'rb')
#		serializedInformation = pickle.load(importFile)
#		restore_q = loads(importFile, self.metadata, base.Session)
#===============================================================================


		tar = tarfile.open(path,'r:bz2') 
		tables = self.metadata.sorted_tables


		for tbl in tables:
		#=======================================================================
		# 
		#	entry = tar.getmember(tbl.name)
		#	print "Importing table %s ..." % entry.name
		#	fileobj = tar.extractfile(entry)
		#	table_dump = loads(fileobj.read(), self.metadata, base.Session)
		#	for data in table_dump:
		#		base.Session.execute(tbl.insert(), dict(**data))
		#	
		#=======================================================================
			entry = tar.getmember(tbl.name)
			print "Importing table %s ..." % entry.name
			fileobj = tar.extractfile(entry)
			table_dump = loads(fileobj.read(), self.metadata, base.Session)
			for data in table_dump:
				base.Session.execute(tbl.insert(), data)
			
		base.Session.commit()

		print "Database imported! Exiting!"


		

		

if __name__ == '__main__':
	
	print("WELLCOME TO MONTERA 2.0")
	print("---")
	print ("date:" + str(datetime.now()))
	print ("database management")
	
	
	print("")
	if len(sys.argv) != 3:
		print ("Usage: dbManagement <import/export> fileLocation")
		print("Exiting...")
		sys.exit(1)
	
	mydbManagement = dbManagement()
		
	command = sys.argv[1].lower().strip()
	path = sys.argv[2].lower().strip()
	
	if command == "import":
		mydbManagement.importDB(path)
	elif command == "export":
		mydbManagement.exportDB(path)
