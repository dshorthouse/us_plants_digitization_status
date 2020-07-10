library(MASS) # provides the parcoord() function that automatically builds parallel coordinates chart
library(tidyverse)

gbif_us_dataset <- read_csv("gbif_institutionCode_summmary/gbif_us_institutionCode_MIDS_2020.07.08.csv")

# Filter down to the top 5 institutions
gbif_us_dataset <- gbif_us_dataset %>% top_n(5, total)

# Sort columns based on MIDS level
gbif_us_dataset <- gbif_us_dataset %>% relocate(has_collectionCode, has_catalogNumber, has_countryCode, has_higherGeography, has_locality, has_eventDate, has_dateIdentified, has_identifiedBy, has_recordedBy, has_image, has_coordinates, has_speciesKey)
  
# Generate percentage of totals as data
percentage_gbif_us_dataset <- gbif_us_dataset[, 1:12] %>%
  mutate(across(everything()), . / gbif_us_dataset$total) 
  
# Remove has_scientificName and has_acceptedNameUsage
# drop_cols <- c("has_scientificName", "has_acceptedNameUsage")
# percentage_gbif_us_dataset <- percentage_gbif_us_dataset %>% select(-one_of(drop_cols))

# Vector color
numeric_institutions <- gbif_us_dataset$institutionCode %>% as.factor() %>% as.numeric()
my_colors <- colors()[numeric_institutions*11]

# Make the graph !
parcoord(percentage_gbif_us_dataset, col = my_colors)

library(GGally)
# add back in the institutionCode
percentage_gbif_us_dataset <- cbind(percentage_gbif_us_dataset, gbif_us_dataset$institutionCode)
colnames(percentage_gbif_us_dataset)[13] <- "institutionCode"
parcordd_plot <- ggparcoord(data = percentage_gbif_us_dataset, columns = 1:12, groupColumn = 13)
parcordd_plot

p <- ggparcoord(data = iris, columns = 1:4, groupColumn = 5, order = "anyClass")
p

