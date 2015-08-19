import sys
from numpy import *

# If the package has been installed correctly, this should work:
import Gnuplot, Gnuplot.funcutils



#gets one or more input files, extracts information and returns it.
#output format:
#array[input files] = entry
#entry: array [end time] = acumulated samples

def processInputFiles (inputFiles):

    allFilesProcesseData = []

    #we process every file with this loop
    for inputFile in inputFiles:
        oneFileProcessedData = []
        allFinishTimes = {}

        myFile = open(inputFile, 'r')

        #process script. We read it line by line, add the submission + queue + execution values, and that's it
        for line in myFile.readlines():
            splittedLine = line.split("\t")
            if len (splittedLine) == 1: #no tabs, no party
                continue

            if splittedLine[1] != "CLEAR":
                continue

            finishTime = float (float (int(splittedLine[3]) + int(splittedLine[4]) + int(splittedLine[5])) / 3600.0)
            samples = int(splittedLine[0])

            allFinishTimes[finishTime] = samples
        myFile.close()

        #sort elements, accumulate the samples, and store them
        acumulatedSamples = 0
        for time in sorted(allFinishTimes):
            acumulatedSamples += allFinishTimes[time]
            #newElemen = [acumulatedSamples, time]
            newElemen = [time, acumulatedSamples]
            oneFileProcessedData.append(newElemen)
        allFilesProcesseData.append(oneFileProcessedData)


    return allFilesProcesseData




def drawPoints(dataToDraw):
    """Draw Montera output."""

    # A straightforward use of gnuplot.  The `debug=1' switch is used
    # in these examples so that the commands that are sent to gnuplot
    # are also output on stderr.
    g = Gnuplot.Gnuplot(debug=1)
    g.title('Montera') # (optional)
    g('set data style linespoints') # give gnuplot an arbitrary command
    g ('set terminal x11 size 1200,800')
    g ('set key right bottom')

    g('set size 1,1')

    #here we build the data to be displayed
    datasets = []  #each line in the graphic is a dataset. We acumulate all here, and then paint them all together
    cont = 0 #just to display a number in the title, not really needed
    for data in dataToDraw: #note that data is an array like data[[x1,y1], [x2,y2], .... , [xn, yn]]
        datasets.append(Gnuplot.Data(data,with_='lp',title='data '+str(cont)))
        cont +=1

    # this is tricky. g.plot wants (param1, param2, ... paramN) as an entry.
    #on the other hand, we have datasets[param1, param2, ..., paramN]
    #the magic is that writing the * before the name, python converts the array into the comma-separated parameter list
    g.plot(*datasets)

    raw_input('Please press return to continue...\n') #this is to stop the execution. If not, it just displays the graphic, deletes it, and finishes execution




# when executed, just run demo():
if __name__ == '__main__':

    if len(sys.argv) < 2:
        print ("One or more input files required")
        print ("python plotResults.py <file1> <file2> ... <fileN>")
        print ("Required libraries: numpy, gnuplot, gnuplot.py")

        sys.exit(-1)


    dataToDraw = processInputFiles(sys.argv[1:])
    drawPoints(dataToDraw)
