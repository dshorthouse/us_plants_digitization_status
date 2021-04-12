
idigbio_changes <- full_dataset %>% group_by(institutionCode) %>% count(total_idig)
idigbio_changes <- idigbio_changes %>% drop_na
write_csv(idigbio_changes, "idigbio_changes.csv")
idigbio_changes <- read_csv("idigbio_changes.csv")
idigbio_mods <- idigbio_changes %>% count(institutionCode, n)
hist(idigbio_mods$nn)
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