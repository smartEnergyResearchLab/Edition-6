

create_new_daily_files <- function(raw_data_folder,
                                   hh_sums_folder,
                                   most_recent_smart_meter_date,
                                   folder_for_daily_csv,
                                   intermediary_folder,
                                   release_version) {
  
  library(data.table)
  library(lubridate)
  library(stringr)
  
  # script containing all author-written functions used here
  source("./functions/smart_meter_prep_functions.R") 
  
  # script defining the error flags and their thresholds etc.
  source("./functions/define_error_flags.R") 
  
  daily <- rbindlist(lapply(paste0(daily_folder, daily_filenames), fread), use.names = TRUE)
  
  name.daily.cols(daily)
  
  format.daily.date.times(daily)
  
  
  hh_sums_list <- lapply(paste0(hh_sums_folder, list.files(hh_sums_folder)), readRDS)
  hh_sums <- process.hh.sums.combined(hh_sums_list) 
  daily <- add.hh.sums(daily, hh_sums)
  
  
  validate.daily.read.time(daily)
  
  
  readDates <- determine_theoretical_read_dates(actual_starts_file, 
                                                inventory_file,
                                                participant_details_file,
                                                most_recent_smart_meter_date)
  inventory <- fread(inventory_file)

  commissioned_gsme <- inventory[deviceType == "GSME" & 
                                   deviceStatus == "Commissioned", 
                                 PUPRN]

  daily <- get.meter.existence(sm_data = daily, 
                               sm_starts = readDates, 
                               commissioned_gsme = commissioned_gsme, 
                               resolution = "daily")
  
  code.errors(daily, error_codes_daily)
  
  convert.gas.daily(daily)
  
  correct.elec.in.kwh(daily, min_n_to_determine_unit_error)
  
  calc.flag.daily.sum.match(daily, 
                            elec_match_limit, 
                            elec_similar_limit,
                            gas_match_limit, 
                            gas_similar_limit)
  
  code.valid.hh.sum.or.daily.read(daily)
  
  select.daily.cols(daily)
  
  # Save rdata file as one (not split annually)
  saveRDS(daily,
          file = paste0(final_rdata_folder, "serl_smart_meter_daily_", release_version, ".RDS"))
  
  
  # Split daily data annually and save as csv files

  daily[, y := year(Read_date_effective_local)]
  daily_years <- split(daily, by = "y", sorted = TRUE)
  
  if(!dir.exists(folder_for_daily_csv)) {
    dir.create(folder_for_daily_csv)
    }
    
  lapply(1:length(daily_years), function(x) {
    # get the filename from the year
    daily_saving_name <- paste0(folder_for_daily_csv,
                                  "serl_smart_meter_daily_",
                                  daily_years[[x]][1, y],
                                  "_", 
                                  release_version, 
                                  ".csv")
    
    # delete the year in the file
    daily_years[[x]][, y := NULL]
    
    # save the final csv
    fwrite(daily_years[[x]],
           file = daily_saving_name)
  })
  
  
  # Get daily errors and sums for rt_summary 
  daily_error_summary <- get.daily.error.summary(daily,
                                                 error_codes_daily,
                                                 n_daily_error_types)
  sums_or_daily_valid <- get.hh.sums.valid.n(daily)
  # save these 2 in the intermediary folder
  saveRDS(daily_error_summary,
          file = paste0(intermediary_folder, "daily_error_summary.RDS"))
  saveRDS(sums_or_daily_valid,
          file = paste0(intermediary_folder, "sums_or_daily_valid.RDS"))
  
  return()
  
}