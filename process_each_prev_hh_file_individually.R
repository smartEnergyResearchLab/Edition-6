

process_each_prev_hh_file_individually <- function(filenumber) {
  
  library(data.table)
  library(stringr)

  # half-hourly data
  f <- prev_hh_files[filenumber]
  
  dt <- do_full_erasure(dt_file = paste0(prev_hh_data_folder, f),
                        erasure_data_file = erasure_data_file,
                        overwrite_file = FALSE,
                        output_table = TRUE)
  
  new_csv_filename <- paste0(substr(f,1, nchar(f) - n_filename_suffix_chars_to_remove),
                             release_version,
                             ".csv")
  fwrite(dt, paste0(final_hh_csv_folder, new_csv_filename))
  
  new_rds_filename <- paste0(substr(f,1, nchar(f) - n_filename_suffix_chars_to_remove - 1),
                             ".RDS")
  saveRDS(dt, paste0(final_hh_rds_folder, new_rds_filename))

  
  
  # half-hourly sums
  g <- prev_hh_sums[filenumber]

  do_full_erasure(dt_file = paste0(prev_hh_sums_folder, g),
                  erasure_data_file = erasure_data_file,
                  overwrite_file = FALSE,
                  output_table = FALSE,
                  new_file = paste0(hh_sums_folder, g))

  
  
  # half-hourly errors
  h <- prev_hh_errors[filenumber]
  
  do_full_erasure(dt_file = paste0(prev_hh_errors_folder, h),
                  erasure_data_file = erasure_data_file,
                  overwrite_file = FALSE,
                  output_table = FALSE,
                  new_file = paste0(hh_error_summaries_folder, h))
  
  
}