
create_new_monthly_hh_files <- function(most_recent_smart_meter_date,
                                        inventory_file,
                                        participant_details_file,
                                        actual_starts_file,
                                        bst_dates_file,
                                        hh_folder,
                                        hh_filenames,
                                        final_hh_csv_folder,
                                        folder_for_hh_rdata,
                                        hh_sums_folder,
                                        hh_error_summaries_folder,
                                        release_version,
                                        save_hh_files = TRUE) {
  
  library(data.table)
  library(lubridate)
  library(stringr)
  library(parallel)
  library(doParallel)
  
  # script containing all author-written functions used here
  source("./functions/smart_meter_prep_functions.R") 
  
  # script defining the error flags and their thresholds etc.
  source("./functions/define_error_flags.R") 
  
  # script to overcome parallel processing issue
  source("./functions/unregister_dopar.R")
  unregister_dopar()
  
  # Load non-smart meter data files
  n_hh_files <- length(hh_filenames)
  
  readDates <- determine_theoretical_read_dates(actual_starts_file, 
                                                inventory_file,
                                                participant_details_file,
                                                most_recent_smart_meter_date)  
  bst_dates <- fread(bst_dates_file)
  setkey(bst_dates, Read_date_effective_local)
  
  inventory <- fread(inventory_file)
  #setnames(inventory, old = "puprn", new = "PUPRN")
  
  commissioned_gsme <- inventory[deviceType == "GSME" & 
                                   deviceStatus == "Commissioned", 
                                 PUPRN]
  

  # Process new half-hourly files
  if(n_hh_files > 2) {
    no_cores <- min(detectCores(logical = TRUE), 
                    n_hh_files + 1)
    cl <- makeCluster(no_cores - 1)
    registerDoParallel(cl)
    clusterEvalQ(cl, {})
    
    variables_to_export <- list('process.each.hh.month.individually',
                                'hh_folder',
                                'hh_filenames',
                                'min_n_to_determine_unit_error',
                                'elec_match_limit',
                                'elec_similar_limit',
                                'gas_match_limit',
                                'gas_similar_limit',
                                'error_codes_hh',
                                'n_hh_error_types',
                                'n_error_types',
                                'save_hh_files')
    
    clusterExport(cl, variables_to_export, envir = environment())
    
    system.time(
      parLapply(
        cl,
        1:n_hh_files,
        fun = process.each.hh.month.individually,
        final_csv_folder = final_hh_csv_folder,
        final_rdata_folder = final_hh_rds_folder,
        hh_sums_folder = hh_sums_folder,
        hh_error_summaries_folder = hh_error_summaries_folder,
        inventory = inventory,
        readDates = readDates,
        bst_dates = bst_dates, 
        commissioned_gsme = commissioned_gsme,
        release_version = release_version
      )
    )
  
    stopCluster(cl)
    
  } else {
    # If 2 or fewer hh files to update don't waste time setting up the parallel environment
    lapply(1:n_hh_files, 
           FUN = process.each.hh.month.individually,
           final_csv_folder = final_hh_csv_folder,
           final_rdata_folder = final_hh_rds_folder,
           hh_sums_folder = hh_sums_folder,
           hh_error_summaries_folder = hh_error_summaries_folder,
           inventory = inventory,
           readDates = readDates,
           bst_dates = bst_dates, 
           commissioned_gsme = commissioned_gsme,
           release_version = release_version)
  }
  
  
  
}


