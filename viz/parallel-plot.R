library(MASS) # provides the parcoord() function that automatically builds parallel coordinates chart
library(tidyverse)

gbif_us_dataset <- read_csv("gbif_institutionCode_summmary/gbif_us_institutionCode_MIDS_2020.07.08.csv")

#generate_column_prop <- function(dataset, col_name) {
  
#}

#gbif_us_dataset <- gbif_us_dataset %>%
#  select(has_image, total) %>%
#  mutate(image_prop = has_image / total, .keep = "all")


# build plot with the largest collection at first (NY: New York Botanical Garden)
ny_gbif_us_dataset <- gbif_us_dataset %>% dplyr::filter(institutionCode == "NY" | institutionCode == "UA")

# Vector color
numeric_institutions <- gbif_us_dataset$institutionCode %>% as.factor() %>% as.numeric()
my_colors <- colors()[numeric_institutions*11]

# Make the graph !
parcoord(gbif_us_dataset[,c(2:5)], col = my_colors)
