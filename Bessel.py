'''
Created on Aug 29, 2012

@author: u5682
'''
import math

def calculateAverage(oldAverage, oldDataSize, newValue):
	acum = oldAverage * oldDataSize
	acum += newValue

	solucion = acum / (oldDataSize +1)

	solucion = math.ceil(100 * solucion)
	solucion = solucion / 100

	return solucion	



def calculateTypicalDeviation(oldAverage, oldDeviation, oldDataSize, newValue):

	if oldDataSize == 0:
		return 0
	
	mediaNueva  = calculateAverage(oldAverage, oldDataSize, newValue)
	varianzaAnterior = math.pow(oldDeviation,2) * (oldDataSize -1)

	elementoNuevo = math.pow((mediaNueva - newValue),2)
	fraccion = (oldDataSize + 1) / oldDataSize 
	elementoNuevo = fraccion * elementoNuevo

	varianzaNueva = varianzaAnterior + elementoNuevo

	total = varianzaNueva / oldDataSize
	solucion = math.sqrt(total)
	solucion = math.ceil(100 * solucion)
	solucion = solucion / 100
	
	return solucion
