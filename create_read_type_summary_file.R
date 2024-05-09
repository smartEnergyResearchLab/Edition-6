

create_read_type_summary_file <- function(intermediary_folder,
                                          hh_sums_folder,
                                          hh_error_summaries_folder,
                                          final_csv_folder,
                                          final_rdata_folder, 
                                          release_version) {
  
  library(data.table)
  library(lubridate)
  library(stringr)
  

  # script containing all author-written functions used here
  source("./functions/smart_meter_prep_functions.R") 
  sapply(list.files("./functions/", full.names = TRUE), source)
  
  hh_error_summary_list <- lapply(list.files(hh_error_summaries_folder, 
                                                  full.names = TRUE),
                                       readRDS)
  hh_error_summary <- rbindlist(hh_error_summary_list, use.names = TRUE)
  
  daily_error_summary <- readRDS(paste0(intermediary_folder, "daily_error_summary.RDS"))
  
  readDates <- determine_theoretical_read_dates(actual_starts_file, 
                                                inventory_file,
                                                participant_details_file,
                                                most_recent_smart_meter_date)
  
  rt_summary <- setup.rt.summary(hh_error_summary, daily_error_summary, readDates)
  
  sums_or_daily_valid <- readRDS(paste0(intermediary_folder, "sums_or_daily_valid.RDS"))
  
  rt_summary <- sums_or_daily_valid[rt_summary]
  
  rt_summary[is.na(validOrHHsumValid) & readType == "DL", 
             validOrHHsumValid := 0]
  
  correct.theoretical.start(rt_summary)
  
  calc.error.percentages(rt_summary)
  
  reorder.rt_summary.cols(rt_summary)
  
  fwrite(rt_summary,
          file = paste0(final_csv_folder, "serl_smart_meter_rt_summary_", release_version, ".csv"))
  
  saveRDS(rt_summary,
         file = paste0(final_rdata_folder, "serl_smart_meter_rt_summary_", release_version, ".RDS"))
  
  return()
}