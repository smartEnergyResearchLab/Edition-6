

perform_full_erasures_smart_meter_input_files <- function(actual_starts,
                                                          inventory,
                                                          participant_details,
                                                          daily,
                                                          hh,
                                                          erasure_data_file) {

  do_full_erasure(dt= NA,
                  dt_file =actual_starts,
                  erasure_data_file,
                  overwrite_file = TRUE)
  
  do_full_erasure(dt= NA,
                  dt_file =inventory,
                  erasure_data_file,
                  overwrite_file = TRUE)
  
  do_full_erasure(dt= NA,
                  dt_file =participant_details,
                  erasure_data_file,
                  overwrite_file = TRUE)
  
  lapply(daily, 
         FUN = do_full_erasure,
         dt = NA,
         new_file = NA,
         erasure_data_file=erasure_data_file,
         overwrite_file = TRUE,
         output_table = FALSE,
         capitalise_puprn = TRUE)
  
  if(length(hh_filepaths) <= 3) {
    # No need to set up parallel environment
    lapply(hh, 
           FUN = do_full_erasure,
           dt = NA,
           new_file = NA,
           erasure_data_file=erasure_data_file,
           overwrite_file = TRUE,
           output_table = FALSE,
           capitalise_puprn = TRUE)
  } else {
    library(parallel)
    library(doParallel)
    unregister_dopar()
    
    no_cores <- min(detectCores(logical = TRUE), 
                    length(hh) + 1)
    cl <- makeCluster(no_cores - 1)
    registerDoParallel(cl)
    clusterEvalQ(cl, {})
    
    #variables_to_export <- list()
    
    #clusterExport(cl, variables_to_export)
    
    parLapply(
      cl,
      hh,
      fun = do_full_erasure,
      dt = NA,
      new_file = NA,
      erasure_data_file= erasure_data_file,
      overwrite_file = TRUE,
      output_table = FALSE,
      capitalise_puprn = TRUE
    )
  }
  
}