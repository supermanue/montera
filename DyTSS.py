'''
Created on Aug 31, 2012

@author: u5682
'''
from base import Session
import base
from DRMAA import *
import sys, os
from GridTask import GridTask

if (base.infrastructureType == "newpilot"):
        from ExecutionManager import createNewGWTemplate as createGWTemplate
else:
        from ExecutionManager import createGWTemplate

from ExecutionManager import removeTaskFromGW
import InformationManager

import math
from scipy import stats
from random import shuffle


class DyTSS(object):
        '''
        classdocs
        '''



        def __init__(self, initialized = False, debug=False):
                '''
                Constructor
                initialized indicates whethet this is a new execution (and all tasks have to be created)
                or the recovery of an old one (so initilization step is not neccesary)
                '''
                self.initialized = initialized  #this controls whether the scheduling has been performed at least once or not
                self.debug = debug

        def createApplicationTasks(self, infrastructure, application, gridTasks):

                print ("-------------")
                print ("-------------")
                print ("DyTSS")

                #in this algorithm, we consider each free slot to be a host (it symplifies the maths)
                goodHosts = infrastructure.getGoodHosts()
                if len (goodHosts) == 0:
                        print ("No available hosts, will not schedule this time")
                        return []

                #print ("")
                #print("Employed hosts:")
                #for host in goodHosts:
                #       print(host.hostname + ", " +  str(host.maxSlotCount) +" slots, "+ str(host.getWhetstones()) + " whetstones")


                newGridTasks = []

                #in the first execution, we consider that the number of free slots on each host
                #is the one stored from the last execution. Thus, we create a task for each of these slots

                gridTaskList = self.createDyTSSTaskList(infrastructure, application)

                if not self.initialized:
                        for host in goodHosts:
                                taskToAdd = self.pickFirstTaskForHost(gridTaskList, host)
                                for i in range(host.maxSlotCount):
                                        newTask= GridTask(None, None, None)
                                        newTask.duplicate(taskToAdd)
                                        newGridTasks.append(newTask)
                                        createGWTemplate(newTask)

                        maxRunningTasks = InformationManager.readMaxRunningTasks()
                        self.priorize(infrastructure, application, newGridTasks)
                        newGridTasks = newGridTasks[:maxRunningTasks]
                        self.initialized = True

                else:
                        currentRunningTasks = 0
                        for host in goodHosts:
                                runningTaks = 0
                                submittedTasks = 0
                                for task in gridTasks:
                                        if task.host is host:
                                                if task.status == "RUNNING":
                                                        runningTaks+= 1
                                                        currentRunningTasks +=1
                                                elif task.status == "SUBMITTED":
                                                        submittedTasks +=1

                                desiredQueuedTasks = max(1,math.ceil(host.currentSlotCount * base.spareTasks))
                                waitingTasksToCreate = desiredQueuedTasks - submittedTasks

                                if waitingTasksToCreate > 0:
                                        print ("In host " + host.hostname + " we have " + str(runningTaks) + " running and " + str(submittedTasks) + " submitted, so we create " + str(waitingTasksToCreate))
                                        for i in range(int(waitingTasksToCreate)):
                                                newTask = self.pickFirstTaskForHost(gridTaskList, host)
                                                newGridTasks.append(newTask)
                                                createGWTemplate(newTask)

                        maxRunningTasks = InformationManager.readMaxRunningTasks()
                        self.priorize(infrastructure, application, newGridTasks)

                        tasksWeWant = int(maxRunningTasks * (1 + base.spareTasks) - currentRunningTasks)

                        print ("Max running tasks: " + str(maxRunningTasks) + "; currentRunningTasks: "+ str(currentRunningTasks) +  " ; taskl we want:  " + str(tasksWeWant) + "; submitting: " + str(len(newGridTasks)))
                        if tasksWeWant > 0:
                                newGridTasks = newGridTasks[:tasksWeWant]
                        else:
                                newGridTasks = []
                                print ("No tasks submitted")


                return newGridTasks




        def createDyTSSTaskList(self, infrastructure, application):

                numIterations = 10 #DEBIG


                #this is the problem size employed in this task scheduling
                #if you consider the reamining samples as the problem to solve in the las execution steps (where thee are very few remaining samples)
                #this algorithm provides bad results, scheduling one sample per task or this like that.
                problemSize  = (application.desiredSamples + application.remainingSamples) /2

                gridTaskList = self.createSimpleTaskList(infrastructure, application, problemSize)
                for iteration in range(numIterations):
                        oldTaskList = gridTaskList

                        (asymtoticPerformance, halfPerformanceLength) = self.calculateInfrastructurePerformance(infrastructure, application, gridTaskList)
                        #(asymtoticPerformance, halfPerformanceLength) = self.calculateInfrastructurePerformanceWithSamples(infrastructure, application, gridTaskList)

                        #===================================================================
                        # print ("createDyTSSTaskList: (asymtoticPerformance, halfPerformanceLength)= ( " + str(asymtoticPerformance) + "," + str(halfPerformanceLength))
                        #===================================================================

                        gridTaskList, anyTaskUpdated = self.GTSSTaskList(infrastructure, halfPerformanceLength, oldTaskList, application, problemSize)


                        #Debug information

                        totalSamples = 0
                        if self.debug :
                                print()
                                print ("Iteration: " + str(iteration))
                                print ("Infrastructure asymtoticPerformance, halfPerformanceLength: " + str(asymtoticPerformance)+ ", " + str(halfPerformanceLength))
                                for gridTask in gridTaskList:
                                        print (gridTask.host.hostname + ": " + str(gridTask.maxSamples) + ", execution time (seconds): " + str(gridTask.expectedExecutionTime()))
                                        totalSamples +=gridTask.maxSamples
                                print ("TOTAL SAMPLES: " + str(totalSamples))


                        if not anyTaskUpdated:
                                break


                return gridTaskList



        #simple task distribution, employed to start the optimization
        def createSimpleTaskList(self, infrastructure, application, problemSize):
                goodHosts = infrastructure.getGoodHosts()


                ##DEBUG
                #=======================================================================
                # if self.debug:
                #       goodHosts = infrastructure.hosts
                #=======================================================================

                hostCounter = 0
                for goodHost in goodHosts:
                        hostCounter+=goodHost.maxSlotCount#TODO: quiza maxSlotCountthistime o algo de eso

                samplesPerTask = problemSize / hostCounter

                newGridTasks = [GridTask(host, None, "applicationExecution",
                                                                minSamples = samplesPerTask,
                                                                maxSamples = samplesPerTask,
                                                                application = application)

                                                for host in goodHosts
                                                for i in range(host.maxSlotCount)]


                #this prints the task distribution

                #=======================================================================
                # totalSamples = 0
                # for gridTask in newGridTasks:
                #       print (gridTask.host.hostname + ": " + str(gridTask.maxSamples))
                #       totalSamples +=gridTask.maxSamples
                #
                # print ("TOTAL SAMPLES IN INIT STEP: " + str(totalSamples))
                # print ("----")
                #=======================================================================

                return newGridTasks



        #this method caculates the infrastructure performance based on the current task distribution.
        #to do so, it imagines that each task is executed 50 times sequentially


        #in order to obtain the performance, it obtains the linear regression of the relationship time /finished tasks
        #that is, the number of finished tasks on a given moment.


        def calculateInfrastructurePerformance(self, infrastructure, application, taskList):
                taskEndTime={}
                fileSize = application.calculateInputFileSize()

                #PROBLEMA:

                #lo que queremos hacer aqui es calcular el rendimiento si hubiera un numero infinito de tareas (el rendimiento asintotico)
                #el problema es que si solo anades un monton de las que tenemos (taskList) una vez que acaban las mas rapidas el
                #rendimiento se reduce (hace una trayectoria curva, como una bala disparada en diagonal) y el resultado es erroneo.
                #la solucion pasa por mirar la tarea que antes acabe con minTaskExecTime y cortar los resultados desde ahi.

                minTaskExecTime = sys.maxint

                for task in taskList:
                        taskExecTime = task.expectedExecutionTime()
                        minTaskExecTime = min(minTaskExecTime, taskExecTime)

                        for i in range(1,51): #51 so it adds 50 elements
                                executionTime = taskExecTime * i

                                if taskEndTime.has_key(executionTime):
                                        taskEndTime[executionTime] += 1
                                else:
                                        taskEndTime[executionTime] = 1

                #convert dictioary into 2 ordered arrays (input of the interpolation function)
                timeArray = taskEndTime.keys()
                timeArray.sort()

                #trim the list as expalined in the previous comment
                minTaskExecTime = 50 * minTaskExecTime
                positionToTrim = timeArray.index(minTaskExecTime)
                timeArray = timeArray[0:positionToTrim]

                finishedTasks  =[]
                finishedTaskCounter = 0


                for elem in timeArray:
                        finishedTaskCounter += taskEndTime[elem]
                        finishedTasks.append(finishedTaskCounter)

                        #===================================================================
                        # debug: print the whole task list employed for the obtention of asymptotic performance
                        #===================================================================

                        #===================================================================
                        # print (str(elem) + "  " + str(finishedTaskCounter))
                        #===================================================================

                #TODO: comprobar si esta bien, tengo que cambiarlo de signo o alguna mierda de esas

                asymtoticPerformance, halfPerformanceLength, r_value, p_value, std_err = stats.linregress(timeArray, finishedTasks)

                return (asymtoticPerformance, halfPerformanceLength)




        def calculateInfrastructurePerformanceWithSamples(self, infrastructure, application, taskList):
                taskEndTime={}
                fileSize = application.calculateInputFileSize()

                #PROBLEMA:

                #lo que queremos hacer aqui es calcular el rendimiento si hubiera un numero infinito de tareas (el rendimiento asintotico)
                #el problema es que si solo anades un monton de las que tenemos (taskList) una vez que acaban las mas rapidas el
                #rendimiento se reduce (hace una trayectoria curva, como una bala disparada en diagonal) y el resultado es erroneo.
                #la solucion pasa por mirar la tarea que antes acabe con minTaskExecTime y cortar los resultados desde ahi.

                minTaskExecTime = sys.maxint

                for task in taskList:
                        taskExecTime = task.host.getQueueTime()
                        #TODO bandiwth, aqui y en el otro lado que esta multiplicado por cero taskExecTime += fileSize * task.host.bandwidth
                        taskExecTime += application.profile.constantEffort / task.host.getWhetstones()
                        taskExecTime += task.maxSamples * application.profile.sampleEffort / task.host.getWhetstones()

                        minTaskExecTime = min(minTaskExecTime, taskExecTime)

                        for i in range(1,51): #51 so it adds 50 elements
                                executionTime = taskExecTime * i

                                if taskEndTime.has_key(executionTime):
                                        taskEndTime[executionTime] += task.maxSamples
                                else:
                                        taskEndTime[executionTime] = task.maxSamples

                #convert dictioary into 2 ordered arrays (input of the interpolation function)
                timeArray = taskEndTime.keys()
                timeArray.sort()

                #trim the list as expalined in the previous comment
                minTaskExecTime = 50 * minTaskExecTime
                positionToTrim = timeArray.index(minTaskExecTime)
                timeArray = timeArray[0:positionToTrim]

                finishedTasks  =[]
                finishedTaskCounter = 0


                for elem in timeArray:
                        finishedTaskCounter += taskEndTime[elem]
                        finishedTasks.append(finishedTaskCounter)

                        #===================================================================
                        # debug: print the whole task list employed for the obtention of asymptotic performance
                        #===================================================================

                        #===================================================================
                        # print (str(elem) + "  " + str(finishedTaskCounter))
                        #===================================================================

                #TODO: comprobar si esta bien, tengo que cambiarlo de signo o alguna mierda de esas

                asymtoticPerformance, halfPerformanceLength, r_value, p_value, std_err = stats.linregress(timeArray, finishedTasks)

                return (asymtoticPerformance, halfPerformanceLength)






        def pickFirstTaskForHost(self, gridTaskList, host):
                foundTask = False #there may be more than one task for a given host, but we only want the first one
                for gridTask in gridTaskList:
                        if gridTask.host == host and not foundTask:
                                return gridTask
                return None



        #implementation of GTSS schedluing algorithm
        #note that we create only one task per host, thus applying only the first step of the algorithm
        #TODO: Este metodo y el siguiente estan mal hechos, ver bien los parametros de entrada/salida, nombres y demas
        def GTSSTaskList(self, infrastructure, halfPerformanceLength, gridTaskList, application, problemSize):

                walltimeRatios = self.calculateWalltimeRatios(infrastructure, application, gridTaskList)

                #para cada site, particulas = F * R_j, con F = I /4_n1/2
                #numberofSamples = samples * ratio [i] / (4 * infrastructure_performance.get_half_performance());

                #=======================================================================
                # print ("")
                # print ("GTSSTaskList")
                # print ("Half performance length: " + str(halfPerformanceLength))
                # print("walltime ratios")
                #=======================================================================

                ratioAcum = 0
                for ratio in walltimeRatios.values():
                        ratioAcum +=ratio
                        #===================================================================
                        # print ("value: " + str(ratio))
                        #===================================================================



                if halfPerformanceLength == 0:
                        print ("Esto es una chapuza pero lo mismo tira.")
                        print ("como solo hay un recurso, el rendimiento asintotico casca, asi que le damos un valor a manopla")
                        halfPerformanceLength = -1

                anyTaskUpdated = False
                for gridTask  in gridTaskList:

                        #calculate  the limits of the task size
                        maxSupportedSamples = self.calculateMaxSamples (gridTask, application)
                        minSupportedSamples = self.calculateMinSamples(gridTask, application, infrastructure)



                        ratio = walltimeRatios[gridTask]
                        oldSamples = gridTask.maxSamples

#                       referenceElement problemSize
#TODO: he cambiado esto para qye mande 4 veces mas samples
                        samples = int ((problemSize * ratio) / (4 * -halfPerformanceLength * ratioAcum))
                        samples = int ((problemSize * ratio) / (4 * -halfPerformanceLength))


                        samples = int ((problemSize * ratio) / ( -halfPerformanceLength))

                        #orig
                        #samples = max (samples, minSupportedSamples)
                        #samples = min (samples, maxSupportedSamples)
                        #ajrm
                        if minSupportedSamples>= maxSupportedSamples:
                                minSupportedSamples = maxSupportedSamples
                                print ("                maxSupportedSamples was smaller than minSupportedSamples, we have corrected that")
                        samples = max(samples, minSupportedSamples)
                        samples = min(samples, maxSupportedSamples)



                        gridTask.maxSamples = samples

                        #if gridTask.expectedExecutionTime() > 86400:
                        #       print ("DyTSS: el tiempo de ejecucion es demasiado largo, esto es raruno (posiblemente ha petado algo)")


                        if oldSamples != samples:
                                anyTaskUpdated = True
                                print ("                Original number of samples was " + str(oldSamples) + ", new is " + str(samples))

                return gridTaskList, anyTaskUpdated

        def calculateWalltimeRatios(self, infrastructure, application, gridTaskList):

                walltimes={}
                minWalltime = float(sys.maxint)
                fileSize = application.calculateInputFileSize()

                for gridTask in gridTaskList:
                        walltime = gridTask.expectedExecutionTime()
                        if      gridTask.host.getBandwidth() > 100:
                                walltime += fileSize / gridTask.host.getBandwidth()

                        walltimes[gridTask] = walltime
                        minWalltime = min(minWalltime, walltime)

                for gridTask, value in walltimes.items():
                        walltimes[gridTask] = minWalltime / value

                return walltimes

        #the minimum number of samples to be executed is determined by the overhead
        #a maximum 10% overhead is allowed
        def calculateMinSamples (self, gridTask, application, infrastructure):


                #esto spn pruebas y cosas pendientes, no vale para nada
                totalComputationalEffort = application.profile.constantEffort + \
                                                                        application.profile.sampleEffort * application.remainingSamples
                hostNumber = len(infrastructure.hosts)
                averageEffortPerHost = totalComputationalEffort/hostNumber

                timeForThisHost = averageEffortPerHost / gridTask.host.getWhetstones()



                #aqui empieza lo util
                #maxOverheadPercentaje=base.maxOverhead

                overhead = gridTask.host.getQueueTime() + \
                                        application.profile.constantEffort / gridTask.host.getWhetstones()
                if gridTask.host.getBandwidth() > 100:
                        overhead +=     application.calculateInputFileSize() / gridTask.host.getBandwidth()


                #ajrm
                minSamples = (100 / base.maxOverhead - 1) *( overhead  * gridTask.host.getWhetstones() /  application.profile.sampleEffort)
                #orig
                #minSamples = (100 / base.maxOverhead) *( overhead  * gridTask.host.getWhetstones() /  application.profile.sampleEffort)

#               minSamples = 1
                return int(minSamples)


        #the maximum number of samples to execute is an execution of one day long
        def calculateMaxSamples (self, gridTask, application):

                overhead = gridTask.host.getQueueTime() + \
                                        application.profile.constantEffort / gridTask.host.getWhetstones()
                if gridTask.host.getBandwidth() > 100:
                        overhead +=     application.calculateInputFileSize() / gridTask.host.getBandwidth()

                #tiempo maximo : base.maxExecutionTime

                timeForSimulations = base.maxExecutionTime  - overhead
                maxSamples = timeForSimulations * gridTask.host.getWhetstones() /  application.profile.sampleEffort

                return int(maxSamples)


        def priorize(self,      infrastructure, application, gridTaskList):
                walltimeRatios = self.calculateWalltimeRatios(infrastructure, application, gridTaskList)
                gridTaskList.sort(key=lambda x: walltimeRatios[x], reverse=True)
