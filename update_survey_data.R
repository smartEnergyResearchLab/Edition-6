
update_survey_data <- function(survey_data_filename_stubs ,
                               data_dictionary_filename_stubs,
                               final_csv_folder,
                               final_pdf_folder,
                               release_version,
                               erasure_data_file = erasure_data_file) {
  
  # Previous data (no need to move it)
  ed_int<-as.integer(str_sub(release_version, 8,9))
  prev_ed_int <- ed_int - 1
  prev_ed_char <- ifelse(nchar(prev_ed_int) == 1, 
                         paste0("0", prev_ed_int),
                         paste0(prev_ed_int)) 
  prev_processed_folder <- paste0("your/folder/here",
                                  prev_ed_char, "/processed_csv/")
  
  # If a new survey comes along that only requires renaming and full erasure, add it here
  old_survey_filepaths <- paste0(prev_processed_folder, survey_data_filename_stubs, 
                                 "_edition", prev_ed_char, ".csv")
  new_survey_filepaths <- paste0(final_csv_folder, survey_data_filename_stubs, "_edition", ed, ".csv")
  
  # Do full erasure and create survey data files with new names in final_csv_folder
  for(i in 1:length(survey_data_filename_stubs)) {
    do_full_erasure(dt_file = old_survey_filepaths[i],
                    erasure_data_file = erasure_data_file,
                    overwrite_file = FALSE,
                    new_file = new_survey_filepaths[i])
  }
  
  # Move and rename data dictionary files
  for(i in 1:length(data_dictionary_filename_stubs)) {
    file.copy(from = paste0(prev_processed_folder, data_dictionary_filename_stubs[i], 
                            "_edition", prev_ed_char, ".csv"),
              to = final_csv_folder)
    file.rename(from = paste0(final_csv_folder, data_dictionary_filename_stubs[i], 
                              "_edition", prev_ed_char, ".csv"),
                to = paste0(final_csv_folder, data_dictionary_filename_stubs[i], 
                            "_", release_version, ".csv"))
  }
  
  # Copy survey pdf copies into new folder
  prev_pdf_folder <- paste0(str_sub(final_pdf_folder, 1, nchar(final_pdf_folder) - 3), prev_ed_char, "/")
  file.copy(from = paste0(prev_pdf_folder, "serl_pilot_recruitment_survey_copy.pdf"),
            to = final_pdf_folder)
  file.copy(from = paste0(prev_pdf_folder, "serl_main_recruitment_survey_copy.pdf"),
            to = final_pdf_folder)
  
  
  return(paste0(length(survey_data_filename_stubs), 
                " survey data files renamed with full erasures and ",
                length(data_dictionary_filename_stubs),
                " data dictionary files renamed. All now in ",
                final_csv_folder))
}
