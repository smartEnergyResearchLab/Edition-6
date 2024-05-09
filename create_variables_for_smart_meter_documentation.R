


create_variables_for_smart_meter_documentation <- function(final_rdata_folder,
                                                           hh_sums_folder,
                                                           release_version,
                                                           folder_for_hh_rdata,
                                                           intermediary_folder) {
  
  library(data.table)
  library(lubridate)
  library(stringr)
  
  rt_summary <- readRDS(paste0(final_rdata_folder, "serl_smart_meter_rt_summary_", release_version, ".RDS"))
  
  all_puprn <- unique(rt_summary$PUPRN)
  n_no_elec <-
    length(all_puprn[!(all_puprn %in% rt_summary[deviceType == "ESME" & 
                                                   percValidOrUnitError > 0, 
                                                 PUPRN])])
  n_no_gas <-
    length(all_puprn[!(all_puprn %in% rt_summary[deviceType == "GPF" 
                                                 & valid > 0, 
                                                 PUPRN])])
  n_pp <- length(all_puprn)
  
  
  # Half-hourly data
  first_read_date_hh <- rt_summary[readType != "DL",
                                   min(firstValidReadDate, na.rm = TRUE)]
  
  last_read_date_hh <- rt_summary[readType != "DL",
                                  max(lastValidReadDate, na.rm = TRUE)]
  
  hh_sums_list <- lapply(paste0(hh_sums_folder, list.files(hh_sums_folder)), readRDS)
  nrows_hh <- sapply(hh_sums_list, function(x) {
    x[, sum(n_hh)]
  })
  
  n_hh_p <- length(unique(rt_summary[readType != "DL" & valid > 0, PUPRN]))
  
  hh_filepaths <- list.files(folder_for_hh_rdata, full.names = TRUE)
  hh <- readRDS(hh_filepaths[1])
  
  ncol_hh <- ncol(hh)
  
  hh_colnames <- colnames(hh)
  hh_col_class <- lapply(hh[1:10], class) # Unclear why it stops at 10. Check documentation.
  
  
  # Daily data
  first_read_date_d <- rt_summary[readType == "DL", 
                                  min(firstValidReadDate, na.rm = TRUE)]
  
  last_read_date_d <- rt_summary[readType == "DL", 
                                 max(lastValidReadDate, na.rm = TRUE)]
  daily <- readRDS(paste0(final_rdata_folder, "serl_smart_meter_daily_", release_version, ".RDS"))
  n_d_p <- length(unique(daily$PUPRN))
  nrow_d <- nrow(daily)
  ncol_d <- ncol(daily)
  
  d_colnames <- colnames(daily)
  d_col_class <- lapply(daily[1:10], class)
  
  
  # Read-type summary stats/variables
  nrow_reads <- nrow(rt_summary)
  ncol_reads <- ncol(rt_summary)
  
  rt_colnames <- colnames(rt_summary)
  rt_col_class <- lapply(rt_summary[1:10], class)
  
  read.type.tab <- rt_summary[valid > 0 | wrongUnits > 0, 
                              .N, 
                              keyby = c("deviceType", "readType")]
  
  # Save 
  save(
    first_read_date_hh,
    last_read_date_hh,
    n_hh_p,
    nrows_hh,
    ncol_hh,
    first_read_date_d,
    last_read_date_d,
    n_d_p,
    nrow_d,
    ncol_d,
    nrow_reads,
    ncol_reads,
    hh_colnames,
    hh_col_class,
    d_colnames,
    d_col_class,
    rt_colnames,
    rt_col_class,
    read.type.tab,
    all_puprn,
    n_no_elec,
    n_no_gas,
    n_pp,
    file = paste0(intermediary_folder, "sm_data_documentation_input.RData")
  )
  
  return()
}







