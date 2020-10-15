library(tidyverse)

# combine data from the two pulls, july and october


idig_oct <- read_csv("cleaned_idigbio_us_institutionCode_MIDS_2020-10-09_agg.csv")
gbif_oct <- read_csv("cleaned_gbif_us_institutionCode_MIDS_2020-10-09_agg.csv")

idig_july <- read_csv("cleaned_idigbio_us_institutionCode_MIDS_2020-07-13-csv_agg.csv")
gbif_july <- read_csv("cleaned_gbif_us_institutionCode_MIDS_2020.07.08_agg.csv")

# make one csv for idig
idig <- bind_rows(list(idig_oct, idig_july))
idig$month <- recode(idig$File, "idigbio_us_institutionCode_MIDS_2020.07.13.csv" = "july", "cleaned_idigbio_us_institutionCode_MIDS_2020-10-09-csv.csv" = "october")
write_csv(idig, "cleaned_idigbio_us_institutionCode_MIDS.csv")

# one csv for gbif
gbif <- bind_rows(list(gbif_oct, gbif_july))
gbif$month <- recode(gbif$File, "gbif_us_institutionCode_MIDS_2020.07.08.csv" = "july", "cleaned_gbif_us_institutionCode_MIDS_2020-10-09-csv.csv" = "october")
write_csv(gbif, "cleaned_gbif_us_institutionCode_MIDS.csv")
