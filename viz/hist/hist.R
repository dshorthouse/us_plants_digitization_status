
full_dataset <- read_csv("gbif_idigbio_institutionCode_summary_cleaned/excel_files_for_tableau/full_dataset.csv")

idigbio_changes <- full_dataset %>% group_by(institutionCode) %>% count(total_idig)
idigbio_changes <- idigbio_changes %>% drop_na
idigbio_changes <- idigbio_changes %>% rename(total_count_observed_num_times = n)

write_csv(idigbio_changes, "idigbio_changes.csv")
idigbio_changes <- read_csv("idigbio_changes.csv")

idigbio_num_changes_per_institution <- idigbio_changes %>% count(institutionCode)
idigbio_num_changes_per_institution <- idigbio_num_changes_per_institution %>% rename(num_count_changes_per_institution = n)

hist(idigbio_num_changes_per_institution$num_count_changes_per_institution)

p <- ggplot(idigbio_num_changes_per_institution, 
       aes(x=num_count_changes_per_institution)) + 
  geom_histogram(binwidth=1, color="black", fill="lightblue") + 
  labs(x="Number of changes in the total number of specimens per institution over three time points", 
       y="Number of institutions", title = "Changes in iDigBio data")
p

table(idigbio_mods$nn)
table(idigbio_mods$nn) / length(idigbio_mods$nn)


gbif_changes <- full_dataset %>% group_by(institutionCode) %>% count(total_gbif)
gbif_changes <- gbif_changes %>% drop_na
write_csv(gbif_changes, "gbif_changes.csv")
gbif_changes <- read_csv("gbif_changes.csv")
gbif_mods <- gbif_changes %>% count(institutionCode, n)
hist(gbif_mods$nn)
table(gbif_mods$nn)
table(gbif_mods$nn) / length(gbif_mods$nn)