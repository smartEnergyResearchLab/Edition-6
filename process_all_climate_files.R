

process_all_climate_files <- function(prev_climate_folder,
                                      final_climate_folder,
                                      raw_climate_folder,
                                      release_version) {
  
  library(lubridate)
  
  # Move and rename previous climate files
  prev_climate_filepaths <- list.files(prev_climate_folder, full.names = TRUE)
  prev_climate_filenames <- list.files(prev_climate_folder, full.names = FALSE)
  
  lapply(1:length(prev_climate_filepaths), function(i) {
    file.copy(from = prev_climate_filepaths[i],
              to = final_climate_folder)
    file.rename(from = paste0(final_climate_folder, prev_climate_filenames[i]),
               to = paste0(final_climate_folder, substr(prev_climate_filenames[i], 1, 
                           nchar(prev_climate_filenames[i]) - 13),
                           release_version, ".csv"))
  })
  
  # any processing and renaming on new climate files
  new_climate_filepaths <- list.files(raw_climate_folder, full.names = TRUE)
  new_climate_filenames <- list.files(raw_climate_folder, full.names = FALSE)
    
  lapply(1:length(new_climate_filenames), function(i) {
    climate <- fread(new_climate_filepaths[i])
    setnames(climate, "date_time", "date_time_utc", skip_absent = TRUE) 
    climate[, analysis_date := ymd(analysis_date)]
    #climate[, date_time_utc := ymd_hms(date_time_utc)] doesn't work on midnight and seems unnecessary (only added March 2023)
    
    if("V1" %in% colnames(climate)) {
      climate[, V1 := NULL]
    }
    
    
    fwrite(climate,paste0(final_climate_folder, "serl_climate_data_",substr(new_climate_filenames[i], 19,22),
                         "_",substr(new_climate_filenames[i], 24,25),"_", release_version, ".csv") )
    
    count_NAs <- sum(is.na(climate))
    count_reads_per_day <- climate[, .N, keyby = analysis_date]
    
    output_table <- data.table(year = count_reads_per_day[1, year(analysis_date)],
                               month = count_reads_per_day[1, month(analysis_date)],
                               days_in_month = days_in_month(paste0(count_reads_per_day[1, year(analysis_date)],
                                                                    "-",
                                                                    count_reads_per_day[1, month(analysis_date)],
                                                                    "-01")),
                               days_with_reads = nrow(count_reads_per_day),
                               min_reads_per_day = min(count_reads_per_day$N),
                               max_reads_per_day = max(count_reads_per_day$N),
                               grid_cells = climate[, length(unique(grid_cell))])
    output_table[, days_with_no_reads := days_in_month - days_with_reads
                        ]
  })
  
  
}

