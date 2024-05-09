

determine_theoretical_read_dates <- function(theoretical_dates_file, 
                                             inventory_file,
                                             participant_details_file,
                                             most_recent_smart_meter_date) {
  
  theoretical_dates <- fread(theoretical_dates_file)
  setnames(theoretical_dates,
           colnames(theoretical_dates),
           c("PUPRN", "deviceType", "readType", "start"))
  setkey(theoretical_dates, start)
  theoretical_dates <- unique(theoretical_dates, 
                              by = c("PUPRN", "deviceType", "readType"))
  
  inventory <- fread(inventory_file)
  

  participant_details <- fread(participant_details_file)
  collection_end_date <- ymd(most_recent_smart_meter_date)
  
  decommission_dates <- get.decommission.dates(inventory)
  consent_end_dates <- participant_details[, .(PUPRN, WoC_CoT_effective_date)]
  consent_end_dates[, WoC_CoT_effective_date := as.Date(WoC_CoT_effective_date,
                                                        format = "%d/%m/%Y")]
  setkeyv(theoretical_dates, c("PUPRN", "deviceType"))
  setkeyv(decommission_dates, c("PUPRN", "deviceType"))
  setkey(consent_end_dates, PUPRN)
  theoretical_dates <- decommission_dates[theoretical_dates]
  theoretical_dates <- consent_end_dates[theoretical_dates]
  theoretical_dates[, theoreticalStart := ymd(start)]
  theoretical_dates[, theoreticalEnd := pmin(WoC_CoT_effective_date - 1,
                                             dateDecommissioned,
                                             collection_end_date,
                                             na.rm = TRUE)]
  
  theoretical_dates <- theoretical_dates[, .(PUPRN, deviceType, readType,
                                             theoreticalStart, theoreticalEnd)]
  
  # remove rows for devices that we only started collection from after the collection_end_date
  theoretical_dates <- theoretical_dates[theoreticalStart <= collection_end_date]
  
  # deal with export read types not treated separately
  export_dates <- copy(theoretical_dates[readType == "EX", ])
  export_dates <- rbind(export_dates, export_dates)
  half_n_export_rows <- nrow(export_dates)/2
  export_dates[1:half_n_export_rows, readType := "AE"]
  export_dates[(half_n_export_rows + 1):(2 * half_n_export_rows), readType := "RE"]
  theoretical_dates_tmp <- theoretical_dates[readType != "EX"]
  theoretical_dates <- rbind(export_dates, theoretical_dates_tmp)
  setkeyv(theoretical_dates, c("PUPRN", "deviceType", "readType"))
  
  theoretical_dates[, daysRange := as.integer(theoreticalEnd - theoreticalStart) + 1L]
  theoretical_dates[, maxPossReads := daysRange * 48L]
  theoretical_dates[readType == "DL", maxPossReads := daysRange]
  
  return(theoretical_dates)
}
