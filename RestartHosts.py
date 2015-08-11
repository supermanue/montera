'''
Created on Aug 31, 2012

@author: u5682
'''
from sqlalchemy import *
from DBDesign import DBDesign
from Host import Host
from Pilot import Pilot
from Application import Application
from PilotResource import PilotResource
from GridTask import GridTask
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

        if len(sys.argv) != 2:
                print ("Usage: Restarthosts.py <clean | delete>")
                print ("clean: reset existing hosts. Delete tmp information. Unban problematic ones")
                print ("delete: Delete everything on Montera database except the application profiles")

        clean = False
        delete = False

        if sys.argv[1] =="clean":
                print ("Updating date of the last failure")
                print ("Setting an old one, so every host can be employed again")
                print (".....")
                clean = True
        elif sys.argv[1] =="delete":
                print ("Deleting everything")
                delete = True
        else:
                print ("Command unknown. Execute RestartHosts.py for information")
                sys.exit(0)





        if clean:
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

                print ("Everything updated")

        elif delete:
                hostList = base.Session.query(Host).all()
                pilotList = base.Session.query(Pilot).all()
                applicationList = base.Session.query(Application).all()
                pilotResourceList = base.Session.query(PilotResource).all()
                gridTaskList = base.Session.query(GridTask).all()

                print ("Delete hosts")
                for elem in hostList:
                        base.Session.delete(elem)

                print ("Delete pilots")
                for elem in pilotList:
                        base.Session.delete(elem)

                print ("Delete application info")
                for elem in applicationList:
                        base.Session.delete(elem)

                print ("Delete pilot info")
                for elem in pilotResourceList:
                        base.Session.delete(elem)

                print ("Delete grid tasks")
                for elem in gridTaskList:
                        base.Session.delete(elem)

                print ("Everything deleted")

        print ("Commiting changes into database")
        base.Session.commit()
        print ("Done")
