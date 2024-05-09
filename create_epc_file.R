

create_epc_file <- function(raw_data_folder,
                            intermediary_folder,
                            prev_raw_data_folder,
                            erasure_data_file,
                            release_version,
                            stop_if_column_name_changes = TRUE) {

# Check for the correct files and directories -----------------------------

  possible_dirs <- list.dirs(raw_data_folder, recursive = FALSE)
  epc_dir <- c(possible_dirs[str_detect(possible_dirs, "epc")],
               possible_dirs[str_detect(possible_dirs, "EPC")])
  
  # Check for EPC directory
  if(length(epc_dir) == 0) {stop(paste0("No EPC data folder in ", raw_data_folder))}
  if(length(epc_dir) > 1) {stop(paste0("No unique EPC data folders in ", raw_data_folder))}
  
  epc_files <- list.files(epc_dir, full.names = TRUE)
  epc_orig_eng_wales_file <- c(epc_files[str_detect(epc_files, "nglish")],
                               epc_files[str_detect(epc_files, "ngland")])
  epc_orig_scotland_file <- c(epc_files[str_detect(epc_files, "cottish")],
                               epc_files[str_detect(epc_files, "cotland")])
  if(length(epc_orig_eng_wales_file) != 1) {
    stop(paste0("One unambiguous EPC file required in ",epc_dir, 
                " for England and Wales (containing 'English' or 'England' in the file name)"))}
  if(length(epc_orig_scotland_file) != 1) {
    stop(paste0("One unambiguous EPC file required in ",epc_dir, 
                " for Scotland (containing 'Scottish' or 'Scotland' in the file name)"))}
  
  
  # Load data and previous edition data for checking ------------------------
  
  # Latest data
  epc_orig_eng_wales <- fread(epc_orig_eng_wales_file)
  epc_eng_wales <- copy(epc_orig_eng_wales)
  setnames(epc_eng_wales, old = "puprn", new = "PUPRN")
  
  epc_orig_scot <- fread(epc_orig_scotland_file)
  epc_scot <- copy(epc_orig_scot)
  setnames(epc_scot, old = "puprn", new = "PUPRN")
  
  
  # Previous data
  possible_prev_dirs <- list.dirs(prev_raw_data_folder, recursive = FALSE)
  prev_epc_dir <- c(possible_prev_dirs[str_detect(possible_prev_dirs, "epc")],
                    possible_prev_dirs[str_detect(possible_prev_dirs, "EPC")])
  prev_epc_files <- list.files(prev_epc_dir, full.names = TRUE)
  prev_epc_orig_eng_wales_file <- c(prev_epc_files[str_detect(prev_epc_files, "nglish")],
                                    prev_epc_files[str_detect(prev_epc_files, "ngland")])
  prev_epc_orig_scotland_file <- c(prev_epc_files[str_detect(prev_epc_files, "cottish")],
                                   prev_epc_files[str_detect(prev_epc_files, "cotland")])
  
  epc_prev_eng_wales <- fread(prev_epc_orig_eng_wales_file)
  epc_prev_scotland <- fread(prev_epc_orig_scotland_file)  
  setnames(epc_prev_eng_wales, old = "puprn", new = "PUPRN")
  setnames(epc_prev_scotland, old = "puprn", new = "PUPRN")
  
  
# Compare new with previous edition EPC data ------------------------------

# Rows and columns
  eng_wal_prev_cols <- colnames(epc_prev_eng_wales)
  eng_wal_cols <- colnames(epc_eng_wales)
  
  scot_prev_cols <- colnames(epc_prev_scotland)
  scot_cols <- colnames(epc_scot)
  
  missing_eng_wal_cols <- setdiff(eng_wal_cols, eng_wal_prev_cols)
  new_eng_wal_cols <- setdiff(eng_wal_prev_cols, eng_wal_cols)
  missing_scot_cols <- setdiff(eng_wal_cols, eng_wal_prev_cols)
  new_scot_cols <- setdiff(eng_wal_prev_cols, eng_wal_cols)
    
  if(stop_if_column_name_changes == TRUE) {
    if(length(new_eng_wal_cols) > 0 |
       length(new_eng_wal_cols) > 0 |
       length(new_scot_cols) > 0 |
       length(missing_scot_cols) > 0) {
      stop(paste0("Changes in column names since previous version. Number of new columns in Eng&Wal EPC data = ", 
                  length(new_eng_wal_cols), 
                  ". Number of new columns in Scotland EPC data = ", 
                  length(new_scot_cols),
                  ". Number of columns missing in new Eng&Wales data = ", 
                  length(missing_eng_wal_cols),
                  ". Number of columns missing in new Scotland data = ", 
                  length(missing_scot_cols),
                  ". Change function input to FALSE if this should be ignored."))
    }
  }
  
  # save the variables that are only in the Scottish data for the documentation
  x <- colnames(epc_eng_wales)
  y <- colnames(epc_scot)
  cols_not_in_Scot = x[!x %in% y]
  cols_only_in_Scot = y[!y %in% x]
  save(
    cols_not_in_Scot, 
    cols_only_in_Scot,
    file = paste0(intermediary_folder, 'epc_documentation_input.RData')
  )
  # Scotland has wallEnergyEff and wallEnvEff don't match these to Eng/Wales walls because Scottish documentation has them as wall
  # Scotland has heatLossCorridoor, leave because it's in the Scottish documentation
  

    # check the data types are the same
    for (col in colnames(epc_scot)){
      old_type = typeof(epc_prev_scotland[[col]])
      new_type = typeof(epc_scot[[col]])
      if (new_type != old_type){
        print(col)
        print(paste('old type', old_type, 'new type', new_type))
      }
    } 
    
    for (col in colnames(epc_eng_wales)){
      old_type = typeof(epc_prev_eng_wales[[col]])
      new_type = typeof(epc_eng_wales[[col]])
      if (new_type != old_type){
        print(col)
        print(paste('old type', old_type, 'new type', new_type))
      }
    } 
    
    # check numeric values are in the same region
    for (col in colnames(epc_scot)){
      new_type = typeof(epc_scot[[col]])
      if ((new_type == 'double') | (new_type == 'integer')){
        print(col)
        print(paste('old max', max(epc_prev_scotland[[col]], na.rm = TRUE), 'new max', max(epc_scot[[col]], na.rm = TRUE)))
        print(paste('old mean', mean(epc_prev_scotland[[col]], na.rm = TRUE), 'new mean', mean(epc_scot[[col]], na.rm = TRUE)))
        
      }
    } # some differences in means and maxes, but generally very similar
  
    # check numeric values are in the same region
    for (col in colnames(epc_eng_wales)){
      new_type = typeof(epc_eng_wales[[col]])
      if ((new_type == 'double') | (new_type == 'integer')){
        print(col)
        print(paste('old max', max(epc_prev_eng_wales[[col]], na.rm = TRUE), 'new max', max(epc_eng_wales[[col]], na.rm = TRUE)))
        print(paste('old mean', mean(epc_prev_eng_wales[[col]], na.rm = TRUE), 'new mean', mean(epc_eng_wales[[col]], na.rm = TRUE)))
        
      }
    } 
    
  # check how many new puprn
  new_eng_wal_puprn <- setdiff(epc_eng_wales$PUPRN, epc_prev_eng_wales$PUPRN) 
  new_scot_puprn <- setdiff(epc_scot$PUPRN, epc_prev_scotland$PUPRN) 
  
  # Check for missing PUPRN (those in the previous dataset that aren't in the new one)
  missing_eng_wal_puprn <- setdiff(epc_prev_eng_wales$PUPRN, epc_eng_wales$PUPRN)
  missing_scot_puprn <- setdiff(epc_prev_scotland$PUPRN, epc_scot$PUPRN) 
  
  # change the formatting of new scottish lodgment dates to match old format
  epc_scot[, lodgementDate := ymd(lodgementDate)]
  epc_scot[, lodgementDate := as.character(format(lodgementDate, "%d/%m/%Y"))]
  
  epc_scot[, inspectionDate := ymd(inspectionDate)]
  epc_scot[, inspectionDate := as.character(format(inspectionDate, "%d/%m/%Y"))]
  
  # join the missing ones into the new data
  epc_eng_wales = rbind(epc_eng_wales, epc_prev_eng_wales[PUPRN %in% missing_eng_wal_puprn, ])
  epc_scot = rbind(epc_scot, epc_prev_scotland[PUPRN %in% missing_scot_puprn, ])
  
  # Perform full erasure
  epc_eng_wales <- do_full_erasure(dt = epc_eng_wales,
                                   erasure_data_file = erasure_data_file,
                                   output_table = TRUE)
  epc_scot <- do_full_erasure(dt = epc_scot,
                              erasure_data_file = erasure_data_file,
                              output_table = TRUE)
  

    
    # check no participants in both data sets
    x <- unique(epc_eng_wales$PUPRN)
    y <- unique(epc_scot$PUPRN)
    x[x %in% y]
    y[y %in% x] # both 0 means no participants appear in both data sets
    
    # make date formats match previous editions and EngWales match Scotland.
    epc_eng_wales[, lodgementDate := as.character(format(lodgementDate, "%d/%m/%Y"))]
    epc_eng_wales[, lodgementDatetime := as.character(as.POSIXct(lodgementDatetime, format = "%d/%m/%Y %H:%M:%S"))]
    epc_eng_wales[, lodgementDatetime := paste(substring(lodgementDatetime, 9, 10), "/",
                                               substring(lodgementDatetime, 6, 7), "/",
                                               substring(lodgementDatetime, 1, 4), " ",
                                               substring(lodgementDatetime, 12, 16), sep = "")]
    epc_eng_wales[, inspectionDate := as.character(format(inspectionDate, "%d/%m/%Y"))]
    

    
    
    # add a column showing which country of EPC each participant has
    epc_eng_wales$epcVersion = 'England and Wales'
    epc_scot$epcVersion = 'Scotland'
    
    epc = rbind(epc_eng_wales, epc_scot, fill = TRUE)
    # move epcVersion to the end of the data frame
    epc = epc[, c(1:80, 82:104, 81)]
    
    # check if there's any data in the addendumText field - if not just remove it
    epc[, .N, keyby = addendumText]
    epc = subset(epc, select = -(addendumText))
    
    # Scottish data sometimes has empty strings instead of NAs where there is no data
    # not mentioned in Scottish documentation so just change these to NA
    epc[epc == ""] <- NA
    
  
    saveRDS(epc, file = paste0(final_rdata_folder, "serl_epc_data_", release_version, ".RDS"))
    fwrite(epc, file = paste0(final_csv_folder, "serl_epc_data_", release_version, ".csv" ))
  
  
}
