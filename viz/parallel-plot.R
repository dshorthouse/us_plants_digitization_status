library(MASS) # provides the parcoord() function that automatically builds parallel coordinates chart
library(tidyverse)

gbif_us_dataset <- read_csv("gbif_institutionCode_summmary/gbif_us_institutionCode_MIDS_2020.07.08.csv")

# Filter down to the top 5 institutions
gbif_us_dataset <- gbif_us_dataset %>% top_n(5, total)



# Generate percentage of totals as data
percentage_gbif_us_dataset <- gbif_us_dataset[, 3:16] %>%
  mutate(across(everything()), . / gbif_us_dataset$total) 
  
# Remove has_scientificName and has_acceptedNameUsage
drop_cols <- c("has_scientificName", "has_acceptedNameUsage")
percentage_gbif_us_dataset <- percentage_gbif_us_dataset %>% select(-one_of(drop_cols))

# Vector color
numeric_institutions <- gbif_us_dataset$institutionCode %>% as.factor() %>% as.numeric()
my_colors <- colors()[numeric_institutions*11]

# Make the graph !
parcoord(percentage_gbif_us_dataset, col = my_colors)

