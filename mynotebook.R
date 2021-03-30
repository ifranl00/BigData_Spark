library(future)
library(sparklyr)

#open spark connection to the cluster
sc <- spark_connect(master = "local", spark_version = "2.3.0")

#reset both folders, source and destination ones to experiment multiple times without problems
if(file.exists("source")) unlink("source", TRUE)
if(file.exists("source-out")) unlink("source-out", TRUE)

#to generate the test file in the folder known as source (input), only one because iterations = 1
stream_generate_test(iterations = 1)

#function read that infer csv file (comma separated values)

read_folder <- stream_read_csv(sc, "source") 
#saved in read_folder the data that return the function that receives sc (spark_connection) and "source": path needed to access the file from the cluster

#The streaming is going to start!

write_output <- stream_write_csv(read_folder, "source-out") #operation: read_folder, path to the file to the cluster to access "source-out"
#stream_write_csv have the job to monitorize the source folder and save the results in output folder


invisible(future(stream_generate_test(interval = 0.5))) #test generation function
#invisible(future(stream_generate_test(interval = 0.2, iterations = 100)))

stream_view(write_output)

#RECOMMENDED: stop shiny app before doing:
#stream_stop(write_output) #stop the stream
#spark_disconnect(sc) #close spark session

