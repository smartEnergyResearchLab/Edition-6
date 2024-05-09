

create_missing_folders_ext <- function(data_folder,
                                   intermediary_folder,
                                   hh_sums_folder,
                                   hh_error_summaries_folder,
                                   final_csv_folder,
                                   final_hh_csv_folder,
                                   final_climate_folder,
                                   final_rdata_folder,
                                   final_hh_rds_folder,
                                   raw_data_folder,
                                   raw_hh_folder,
                                   raw_daily_folder,
                                   raw_climate_folder,
                                   prev_hh_data_folder,#?
                                   prev_hh_sums_folder,#?
                                   prev_hh_errors_folder,#?
                                   markdown_folder,
                                   word_doc_folder,
                                   final_pdf_folder, 
                                   release_version) {
  
  n_new_folders <- 0
  
  if(!dir.exists(data_folder)) {
    dir.create(data_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(intermediary_folder)) {
    dir.create(intermediary_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(hh_sums_folder)) {
    dir.create(hh_sums_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(hh_error_summaries_folder)) {
    dir.create(hh_error_summaries_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(final_csv_folder)) {
    dir.create(final_csv_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(final_hh_csv_folder)) {
    dir.create(final_hh_csv_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(final_climate_folder)) {
    dir.create(final_climate_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(final_rdata_folder)) {
    dir.create(final_rdata_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(final_hh_rds_folder)) {
    dir.create(final_hh_rds_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(raw_data_folder)) {
    dir.create(raw_data_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(raw_hh_folder)) {
    dir.create(raw_hh_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(raw_daily_folder)) {
    dir.create(raw_daily_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(raw_climate_folder)) {
    dir.create(raw_climate_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(prev_hh_data_folder)) {
    dir.create(prev_hh_data_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(prev_hh_sums_folder)) {
    dir.create(prev_hh_sums_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(prev_hh_errors_folder)) {
    dir.create(prev_hh_errors_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(word_doc_folder)) {
    dir.create(word_doc_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(final_pdf_folder)) {
    dir.create(final_pdf_folder)
    n_new_folders <- n_new_folders + 1
  }
  if(!dir.exists(markdown_folder)) {
    dir.create(markdown_folder)
    n_new_folders <- n_new_folders + 1
    prev_ed <- as.integer(str_sub(release_version, 8,9)) 
    if(nchar(prev_ed) == 1) {
      prev_ed_char <- paste0("0", prev_ed)
    } else {
      prev_ed_char <- as.character(prev_ed)
    }
    file.copy(from = paste0(substr(getwd(), 1, nchar(getwd()) - 2), prev_ed_char, 
                            "/documentation/", "SERL_word_template_portrait1.docx"),
              to = paste0(getwd(), "/documentation/"))
    file.copy(from = paste0(substr(getwd(), 1, nchar(getwd()) - 2), prev_ed_char, 
                            "/documentation/", "SERL_word_template_landscape1.docx"),
              to = paste0(getwd(), "/documentation/"))
  }
  print(paste0(n_new_folders, " new folders created"))
  return()
}



