'''
Created on 24/04/2013

@author: supermanue
'''


from __future__ import division
import sys, os
from datetime import datetime

import xml.dom.minidom
from xml.dom.minidom import Node

from sqlalchemy import *
from base import Base
from sqlalchemy.orm import relationship, backref
from base import Session


class Parameter(Base):
   
    __tablename__ = 'parameters'
    id = Column(Integer, primary_key=True)
    parameter = Column(String)
    status  = Column(String)
    
    creationDate = Column(DateTime)
    executionStartDate = Column(DateTime)
    endDate = Column(DateTime)
    
    
    def __init__(self, parameter):
        '''
        Constructor
        '''
        self.parameter = parameter
        self.status = "WAITING"        
        self.creationDate = datetime.now()

    