library(MASS) # provides the parcoord() function that automatically builds parallel coordinates chart
library(tidyverse)
library(GGally)

gbif_us_dataset <- read_csv("gbif_institutionCode_summmary/gbif_us_institutionCode_MIDS_2020.07.08.csv")

############# TOP 5 #############

# Filter down to the top 5 institutions
gbif_us_dataset <- gbif_us_dataset %>% top_n(5, total)

generate_mass_parcoord_plot <- function(gbif_us_dataset) {
  # Sort columns based on MIDS level
  gbif_us_dataset <- gbif_us_dataset %>% relocate(has_collectionCode, has_catalogNumber, has_countryCode, has_higherGeography, has_locality, has_eventDate, has_dateIdentified, has_identifiedBy, has_recordedBy, has_image, has_coordinates, has_speciesKey)
  
  # Generate percentage of totals as data
  percentage_gbif_us_dataset <- gbif_us_dataset[, 1:12] %>%
    mutate(across(everything()), . / gbif_us_dataset$total) 
  
  # Vector color
  numeric_institutions <- gbif_us_dataset$institutionCode %>% as.factor() %>% as.numeric()
  my_colors <- colors()[numeric_institutions*11]
  
  # Make the graph !
  parcoord(percentage_gbif_us_dataset, col = my_colors)
}

mass_parcoord_plot <- generate_mass_parcoord_plot(gbif_us_dataset)
mass_parcoord_plot

generate_ggally_parcoord_plot <- function(gbif_us_dataset) {
  # Sort columns based on MIDS level
  gbif_us_dataset <- gbif_us_dataset %>% relocate(has_collectionCode, has_catalogNumber, has_countryCode, has_higherGeography, has_locality, has_eventDate, has_dateIdentified, has_identifiedBy, has_recordedBy, has_image, has_coordinates, has_speciesKey)
    
  # Generate percentage of totals as data
  percentage_gbif_us_dataset <- gbif_us_dataset[, 1:12] %>%
    mutate(across(everything()), . / gbif_us_dataset$total) 
  
  # add back in the institutionCode
  percentage_gbif_us_dataset <- cbind(percentage_gbif_us_dataset, gbif_us_dataset$institutionCode)
  colnames(percentage_gbif_us_dataset)[13] <- "institutionCode"
  parcordd_plot <- ggparcoord(data = percentage_gbif_us_dataset, columns = 1:12, groupColumn = 13)
  return(parcordd_plot)
}

ggally_parcordd_plot <- generate_ggally_parcoord_plot(gbif_us_dataset)
ggally_parcordd_plot

############# BOTTOM 5 #############

gbif_us_dataset <- read_csv("gbif_institutionCode_summmary/gbif_us_institutionCode_MIDS_2020.07.08.csv")

# Filter down to the bottom 5 institutions
gbif_us_dataset <- gbif_us_dataset %>% top_n(-5, total)
# Remove Triplehord insect collection
gbif_us_dataset <- gbif_us_dataset %>% filter(institutionCode != "C.A. Triplehorn Insect Collection, Ohio State University, Columbus, OH (OSUC)")

mass_parcoord_plot <- generate_mass_parcoord_plot(gbif_us_dataset)
mass_parcoord_plot

ggally_parcordd_plot <- generate_ggally_parcoord_plot(gbif_us_dataset)
ggally_parcordd_plot


############# FULL DATASET #############

gbif_us_dataset <- read_csv("gbif_institutionCode_summmary/gbif_us_institutionCode_MIDS_2020.07.08.csv")

generate_ggally_parcoord_plot <- function(gbif_us_dataset) {
  # Sort columns based on MIDS level
  gbif_us_dataset <- gbif_us_dataset %>% relocate(has_collectionCode, has_catalogNumber, has_countryCode, has_higherGeography, has_locality, has_eventDate, has_dateIdentified, has_identifiedBy, has_recordedBy, has_image, has_coordinates, has_speciesKey)
  
  # Generate percentage of totals as data
  percentage_gbif_us_dataset <- gbif_us_dataset[, 1:12] %>%
    mutate(across(everything()), . / gbif_us_dataset$total) 
  
  # add back in the institutionCode
  percentage_gbif_us_dataset <- cbind(percentage_gbif_us_dataset, gbif_us_dataset$institutionCode)
  colnames(percentage_gbif_us_dataset)[13] <- "institutionCode"
  parcordd_plot <- ggparcoord(data = percentage_gbif_us_dataset, columns = 1:12, scale = "uniminmax", alphaLines = 0.05) + theme_bw()
  return(parcordd_plot)
}

ggally_parcordd_plot <- generate_ggally_parcoord_plot(gbif_us_dataset)
ggally_parcordd_plot




