# EDIService

This is to work around the Kiwiplan KMC bug that the files are not being picked up by the listner process and does not respect 
the date and time stamp of the file when it was created. So the Kiwiplan process picks up the file in any order insted of doing FIFO.