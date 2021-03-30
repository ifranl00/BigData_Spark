library(future)
library(sparklyr)
library(dplyr,warn.conflicts = FALSE)



#open spark connection to the cluster
sc <- spark_connect(master = "local", spark_version = "2.3.0")

#reset both folders, source and destination ones to experiment multiple times without problems
if(file.exists("source")) unlink("source", TRUE)
if(file.exists("source-out")) unlink("source-out", TRUE)

#to generate the test file in the folder known as source (input), only one because iterations = 1
stream_generate_test(iterations = 1)

#function read that infer csv file (comma separated values)
#saved in read_folder the data that return the function that receives sc (spark_connection) and "source": path needed to access the file from the cluster
read_folder <- stream_read_csv(sc, "source") 

#processing data begins
process_stream <- read_folder %>%
  mutate(x = as.double(x)) %>% # ft_binarizer() does not accept integer
  ft_binarizer( #feature transformation
    input_col = "x", #name of the input column
    output_col = "over", #name of the ouput column
    threshold = 400 # threshold to binarize continuous features
  )

#stream_write_csv have the job to monitoring the new process_stream folder and save the results in output folder
#operation: read_folder, path to the file to the cluster to access "source-out"
write_output <- stream_write_csv(process_stream, "source-out")

#invisible(future(stream_generate_test(interval = 0.2, iterations = 100)))
invisible(future(stream_generate_test(interval = 0.2, iterations = 100)))

#stream_view(write_output)

#Read a tabular data file into a Spark DataFrame.
spark_read_csv(
  sc, #spark connection
  "stream", #name to the new table
  "source-out", # path to the the folder to access from the cluster
  memory = FALSE # the table is not saved in the cache
  ) %>%
    group_by(over) %>% 
    tally()

stream_stop(write_output) #stop the stream
spark_disconnect(sc)#close spark session