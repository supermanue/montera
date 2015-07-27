'''
Created on 19/04/2013

@author: supermanue
'''

from bottle import route, run, template

from sqlalchemy import *
from base import Base
from sqlalchemy.orm import relationship, backref
from base import Session
from datetime import datetime

from Parameter import Parameter

class Server (object):

	def __init__(self):
		counter = 0
		
	
	
	
		
@route('/getTask')
def getTask():
	newParameter = Session.query(Parameter).filter(Parameter.status=="WAITING").first()
	if newParameter == None:
		return template('{{id}}', id=-1)

	
	newParameter.status="RUNNING"
	newParameter.executionStartDate = datetime.now()
	Session.add(newParameter)
	Session.commit()
	return template('{{id}}', id=newParameter.id)



@route('/failedTask/:idInput')
def failedTask(idInput=-1):
	#por si acaso
	try:
		id = int(idInput)
	except:
		return template('-1')

	failedParameter = Session.query(Parameter).filter(Parameter.id==id).first()
	failedParameter.status="WAITING"
	failedParameter.executionStartDate = None
	Session.add(failedParameter)
	Session.commit()
	return template('{{id}}', id=failedParameter.id)


		
@route('/finishedTask/:idInput')
def finishedTask(idInput=-1):
	#por si acaso
	try:
		id = int(idInput)
	except:
		return template('-1')

	finishedParameter = Session.query(Parameter).filter(Parameter.id==id).first()
	finishedParameter.status="DONE"
	finishedParameter.executionEndDate = datetime.now()
	Session.add(finishedParameter)
	Session.commit()
	#TODO: en algun sitio habria que verificar los datos de salida antes del done
	return template('{{id}}', id=finishedParameter.id)


run(host='localhost', port=8080)
