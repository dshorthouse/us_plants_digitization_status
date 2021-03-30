library(tidyverse)

aggregate_counts_sum_return_new_dataset <- function(file_name) {
  setwd("~/us_plants_digitization_status/gbif_idigbio_institutionCode_summary_cleaned/non_agg_csv")
  dataset <- read_csv(file_name)
  dataset <- dataset %>% group_by(institutionCode) %>% summarise_all(funs(sum))
  File <- rep(file_name, nrow(dataset))
  dataset <- cbind(dataset, File)
  setwd("../agg_csv")
  write_csv(dataset, paste(strsplit(file_name, ".csv")[[1]][1], "csv_agg.csv", sep="-"))
}


# remove duplicates
# idigbio_us_dataset <- read_csv("cleaned_idigbio_us_institutionCode_MIDS_2020-12-15-csv.csv")
idigbio_us_dataset <- aggregate_counts_sum_return_new_dataset("cleaned_idigbio_us_institutionCode_MIDS_2020-12-15-csv.csv")
# write_csv(idigbio_us_dataset, "cleaned_idigbio_us_institutionCode_MIDS_2020-12-15-csv_agg.csv")

# import the GBIF data dump
# gbif_us_dataset <- read_csv("cleaned_gbif_us_institutionCode_MIDS_2020-12-06-csv.csv")
gbif_us_dataset <- aggregate_counts_sum_return_new_dataset("cleaned_gbif_us_institutionCode_MIDS_2020-12-06-csv.csv")
# write_csv(gbif_us_dataset, "cleaned_gbif_us_institutionCode_MIDS_2020-12-06-csv_agg.csv")
