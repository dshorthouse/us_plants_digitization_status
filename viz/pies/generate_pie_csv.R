
### Data prep for pie charts

library(tidyverse)

idigbio_pivoted_us_dataset <- pivot_longer(idigbio_us_dataset, -institutionCode, names_to = "variable", values_to = "counts")
gbif_pivoted_us_dataset <- pivot_longer(gbif_us_dataset, -institutionCode, names_to = "variable", values_to = "counts")

no_totals_pivot <- function(us_dataset, pivoted_us_dataset) {
  # Generate  has_no totals as data
  total_minus_us_dataset <- us_dataset[, 3:ncol(us_dataset)] %>%
    mutate(across(everything()), us_dataset$total - .) %>% cbind(., us_dataset$institutionCode)
  colnames(total_minus_us_dataset) <- colnames(total_minus_us_dataset) %>% gsub("has", "has_no", .)
  colnames(total_minus_us_dataset)[ncol(total_minus_us_dataset)] <- "institutionCode" # clean up yucky column name after cbind operation
  
  total_minus_pivoted_us_dataset <- pivot_longer(total_minus_us_dataset, -institutionCode, names_to = "variable", values_to = "counts")
  pivoted_total_minus_us_dataset <- bind_rows(pivoted_us_dataset, total_minus_pivoted_us_dataset)
  #gbif_pivoted_us_dataset <- pivot_longer(gbif_us_dataset, -institutionCode, names_to = "variable", values_to = "counts")
  # write_csv(gbif_pivoted_total_minus_us_dataset, "gbif_pivoted_total_minus_us_dataset.csv")
  return(pivoted_total_minus_us_dataset)
}

gbif_pivoted_total_minus_us_dataset <- no_totals_pivot(gbif_us_dataset, gbif_pivoted_us_dataset)
gbif_pivoted_total_minus_us_dataset

idigbio_pivoted_total_minus_us_dataset <- no_totals_pivot(idigbio_us_dataset, idigbio_pivoted_us_dataset)
idigbio_pivoted_total_minus_us_dataset

merged_pivoted_totals_us_dataset <- bind_rows(idigbio = idigbio_pivoted_total_minus_us_dataset, gbif = gbif_pivoted_total_minus_us_dataset, .id = "data_provider")
write_csv(merged_pivoted_totals_us_dataset, "merged_pivoted_totals_us_dataset.csv")
