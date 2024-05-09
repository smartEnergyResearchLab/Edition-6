

get_prev_climate_folder <- function(ed,
                                    top_level_folder = "your/folder/here",
                                    foldername_after_edition = "processed_csv/serl_climate_data_edition") {
  
  library(stringr)
  ed_int <- as.integer(ed)
  prev_ed_int <- ed_int - 1
  prev_ed_char <- ifelse(nchar(prev_ed_int) == 1, 
                         paste0("0", prev_ed_int),
                         paste0(prev_ed_int)) 
  
  return(paste0(top_level_folder, "edition", prev_ed_char, "/", foldername_after_edition, prev_ed_char, "/"))
}