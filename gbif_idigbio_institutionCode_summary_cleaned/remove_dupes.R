library(tidyverse)

aggregate_counts_sum_return_new_dataset <- function(dataset) {
  file_name <- dataset$File[1]
  dataset <- dataset %>% select(-File) %>% group_by(institutionCode) %>% summarise_all(funs(sum))
  File <- rep(file_name,nrow(dataset))
  dataset <- cbind(dataset, File)
  return(dataset)
}

# remove duplicates
idigbio_us_dataset <- read_csv("cleaned_idigbio_us_institutionCode_MIDS_2020-07-13-csv.csv")
idigbio_us_dataset <- aggregate_counts_sum_return_new_dataset(idigbio_us_dataset)
write_csv(idigbio_us_dataset, "cleaned_idigbio_us_institutionCode_MIDS_2020-07-13-csv_agg.csv")

# import the GBIF data dump
gbif_us_dataset <- read_csv("cleaned_gbif_us_institutionCode_MIDS_2020.07.08.csv")
gbif_us_dataset <- aggregate_counts_sum_return_new_dataset(gbif_us_dataset)
write_csv(gbif_us_dataset, "cleaned_gbif_us_institutionCode_MIDS_2020.07.08_agg.csv")