

update_previous_hh_files <- function(prev_hh_data_folder,
                                      prev_hh_sums_folder,
                                      prev_hh_errors_folder,
                                      prev_hh_rds_folder,
                                      final_hh_csv_folder,
                                      final_hh_rds_folder,
                                      hh_sums_folder,
                                      hh_error_summaries_folder,
                                      erasure_data_file,
                                      prev_erasure_data_file,
                                       release_version,
                                      n_filename_suffix_chars_to_remove = 13) {
  
  prev_hh_files <- list.files(prev_hh_data_folder, full.names = FALSE)
  prev_hh_sums <- list.files(prev_hh_sums_folder, full.names = FALSE)
  prev_hh_errors <- list.files(prev_hh_errors_folder, full.names = FALSE)
  
  # Check we have all the files we need
  if(length(prev_hh_files) == 0) {
    stop(paste0("No previous half-hourly files in ", prev_hh_data_folder))
  }
  if(length(prev_hh_sums) == 0) {
    stop(paste0("No previous half-hourly sums files in ", prev_hh_sums_folder))
  }
  if(length(prev_hh_errors) == 0) {
    stop(paste0("No previous half-hourly error files in ", prev_hh_errors_folder))
  }
  if((length(prev_hh_files) != length(prev_hh_sums)) |
     (length(prev_hh_sums) != length(prev_hh_errors)) |
     (length(prev_hh_errors) != length(prev_hh_files))) {
    stop("Need the same number of half-hourly data files as hh_sums files and as hh_errors files")
  }
  
  
  # First check if there are any full erasures. If not we save a lot of time. 
  prev_erasures <- fread(prev_erasure_data_file)
  new_erasures <- fread(erasure_data_file)
  
  if(length(c(setdiff(prev_erasures, new_erasures),
              setdiff(new_erasures, prev_erasures))) == 0) {
    # ie no new full erasures
    # just rename and copy the files
    lapply(prev_hh_files, function(f) {
      # csv files
      new_csv_filename <- paste0(substr(f,1, nchar(f) - 13), release_version, ".csv")
      file.copy(from = paste0(prev_hh_data_folder, f),
                to = final_hh_csv_folder)
      file.rename(from = paste0(final_hh_csv_folder, f),
                  to = paste0(final_hh_csv_folder, new_csv_filename))
      # rds files (need to go and find them in the previous edition)
      file.copy(from = paste0(prev_hh_rds_folder, substr(f, 1, nchar(f) - 14), ".RDS"),
                to = final_hh_rds_folder)
    })
    
    lapply(prev_hh_sums, function(f) {
      file.copy(from = paste0(prev_hh_sums_folder, f),
                to = hh_sums_folder)
    })
    
    lapply(prev_hh_errors, function(f) {
      file.copy(from = paste0(prev_hh_errors_folder, f),
                to = hh_error_summaries_folder)
    })
    
    return("No new full erasure requests detected, previous files copied into new edition folders and renamed")
    
  } else {
    library(parallel)
    library(doParallel)
    
    # Set up parallel environment
    
    # script to overcome parallel processing issue
    source("./functions/unregister_dopar.R")
    unregister_dopar()
    
    n_hh_files <- length(prev_hh_files)
    
    no_cores <- min(detectCores(logical = TRUE), 
                    n_hh_files + 1)
    cl <- makeCluster(no_cores - 1)
    registerDoParallel(cl)
    clusterEvalQ(cl, {})
    
    variables_to_export <- list('prev_hh_files',
                                'prev_hh_sums',
                                'prev_hh_errors',
                                'prev_hh_data_folder',
                                'prev_hh_sums_folder',
                                'prev_hh_errors_folder',
                                'final_hh_csv_folder',
                                'final_hh_rds_folder',
                                'hh_sums_folder',
                                'hh_error_summaries_folder',
                                'n_filename_suffix_chars_to_remove',
                                'release_version',
                                'erasure_data_file',
                                'do_full_erasure')
    
    clusterExport(cl, variables_to_export, envir = environment())
    
    system.time(
      parLapply(
        cl,
        1:n_hh_files,
        fun = process_each_prev_hh_file_individually)
    )
    stopCluster(cl)
    
    return(paste0(n_hh_files, " previous half-hourly files have been processed"))
  }

}

