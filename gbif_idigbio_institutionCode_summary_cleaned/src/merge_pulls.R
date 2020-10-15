library(tidyverse)

# combine data from the two pulls, july and october


idig_oct <- read_csv("../agg_month/cleaned_idigbio_us_institutionCode_MIDS_2020-10-09_agg.csv")
gbif_oct <- read_csv("../agg_month/cleaned_gbif_us_institutionCode_MIDS_2020-10-09_agg.csv")

idig_july <- read_csv("../agg_month/cleaned_idigbio_us_institutionCode_MIDS_2020-07-13-csv_agg.csv")
gbif_july <- read_csv("../agg_month/cleaned_gbif_us_institutionCode_MIDS_2020.07.08_agg.csv")

# make one csv for idig
idig <- bind_rows(list(idig_oct, idig_july))
idig$month <- recode(idig$File, "idigbio_us_institutionCode_MIDS_2020.07.13.csv" = "july", "cleaned_idigbio_us_institutionCode_MIDS_2020-10-09-csv.csv" = "october")
write_csv(idig, "../agg_month_csv/cleaned_idigbio_us_institutionCode_MIDS.csv")

# one csv for gbif
gbif <- bind_rows(list(gbif_oct, gbif_july))
gbif$month <- recode(gbif$File, "gbif_us_institutionCode_MIDS_2020.07.08.csv" = "july", "cleaned_gbif_us_institutionCode_MIDS_2020-10-09-csv.csv" = "october")
write_csv(gbif, "../agg_month_csv/cleaned_gbif_us_institutionCode_MIDS.csv")


# need to do a full outer join of all of the datasets
oct <- idig_oct %>% full_join(gbif_oct, by = "institutionCode", suffix = c("_idig", "_gbif"))

# add month column
oct$month <- rep("october", nrow(oct))

july <- idig_july %>% full_join(gbif_july, by = "institutionCode", suffix = c("_idig", "_gbif"))
july$month <- rep("july", nrow(july))

full_dataset <- bind_rows(list(oct, july))
write_csv(full_dataset, "../excel_files_for_tableau/full_dataset.csv")
