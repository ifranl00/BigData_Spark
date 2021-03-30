library(future)
library(sparklyr)
library(dplyr, warn.conflicts = FALSE)

sc <- spark_connect(master = "local", spark_version = "2.3.0")

if(file.exists("source")) unlink("source", TRUE)

stream_generate_test(iterations = 1)
read_folder <- stream_read_csv(sc, "source") 

process_stream <- read_folder %>%
  stream_watermark() %>% #add a column with the current time
  group_by(timestamp) %>% 
  summarise(
    max_x = max(x, na.rm = TRUE),
    min_x = min(x, na.rm = TRUE),
    count = n()
  )

write_output <- stream_write_memory(process_stream, name = "stream")

invisible(future(stream_generate_test()))

#tbl(sc, "stream")

#stream_stop(write_output)
#spark_disconnect(sc)