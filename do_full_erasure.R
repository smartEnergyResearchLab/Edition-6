#' Function to perform full erasure on a data table, csv file, or .RDS data table. Full erasure
#'   means removing PUPRNs from the table which appear in the 'erasure_data_file'
#' 
#' If given a data table, outputs a data table with PUPRNs in the erasure_data_file removed
#' If given a file and overwrite_file = TRUE, overwrites the original file without the PUPRNs in 
#'   the erasure_data_file.
#' If new_file specifies a .csv or .RDS filepath then a new file will be created of the 
#'   data table minus the PUPRNs specified in erasure_data_file with this file path. Possible with either
#'   dt or dt_file as an input. 
#' 
#' 
#' @param dt Optional: data table with one column named 'PUPRN'. Can be left as NA if giving a file instead
#' @param dt_file Alternative to specifying dt, a .csv or .RDS filepath to a table with one column named 'PUPRN'
#' @param erasure_data_file csv file with 1 column called 'PUPRN' specifying which PUPRN to fully erase
#' @param overwrite_file TRUE if dt_file != NA and we want to overwrite the dt_file after performing full erasure
#' @param new_file filepath to a .RDS or .csv file if we want to create a new file
#' @param output_table TRUE if we want to output a data.table with the PUPRNs removed
#' @param capitalise_puprn Some files have column name 'puprn' setting this TRUE renames the column to PUPRN
#' 
#' @author Ellen Zapata-Webborn \email{e.webborn@ucl.ac.uk} (original)


do_full_erasure <- function(dt = NA,
                            dt_file = NA,
                            erasure_data_file,
                            overwrite_file = FALSE,
                            new_file = NA, 
                            output_table = FALSE,
                            capitalise_puprn = TRUE) {
  
  library(data.table)
  library(stringr)
  
  if(!file.exists(erasure_data_file)) {
    stop(paste0("No full erasure file found (looking for ", erasure_data_file))
  }
  
  # Load participants to erase
  erasures <- fread(erasure_data_file)
  if(!"PUPRN" %in% colnames(erasures)) {
    stop("PUPRN needs to be a column name in the erasures data")
  } else if(nrow(erasures) == 0) {
    stop("No participants requesting full erasure (file has zero rows)")
  }
  
  if(!is.data.frame(dt)) {
    dt_ext <- str_sub(dt_file, -3)
    if(dt_ext == "csv") {
      dt <- fread(dt_file)
    } else if(dt_ext == "RDS") {
      dt <- readRDS(dt_file)
    } else {
    stop("dt_file (the file we're performing full erasure on) needs to be a .csv or .RDS file")
    }
  }
  
  if("puprn" %in% colnames(dt)) {
    if(capitalise_puprn == TRUE) {
      setnames(dt, "puprn", "PUPRN")
      dt <- dt[!PUPRN %in% erasures$PUPRN]
    } else {
      dt <- dt[!puprn %in% erasures$PUPRN]
    }
  } else {
    dt <- dt[!PUPRN %in% erasures$PUPRN]
  }
  
  
  if (overwrite_file == TRUE & !is.na(dt_file)) {
      if (dt_ext == "csv") {
        fwrite(dt, dt_file)
      } else {
        saveRDS(dt, dt_file)
      }
      message("Full erasure complete and file overwitten")
      return()
    } else if (!is.na(new_file)) {
      if (str_sub(new_file,-3) == "csv") {
        fwrite(dt, new_file)
      } else if (str_sub(new_file,-3) == "RDS") {
        saveRDS(dt, new_file)
      } else {
        stop("new_file (function input) needs to be .RDS or .csv filepath")
      }
    }
  if(output_table == TRUE) {
    return(dt)
  } else{return()}

}