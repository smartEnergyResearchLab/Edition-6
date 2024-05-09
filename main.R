

# From this file all code can be accessed and run to create the SERL Observatory
#  datasets. Some sections require previous sections to have been run in order
#  for the correct input data to exist. Details are given in each section. 


# Setup -------------------------------------------------------------------

# Update the most recent smart meter collection date
most_recent_smart_meter_date <- "2023-12-31"

library(stringr)
library(data.table)
options(datatable.fread.input.cmd.message = FALSE)

sapply(list.files("./functions/", full.names = TRUE), source)
if(system.file(package = 'bit64') == "") {
  stop("Please install package 'bit64' before continuing")
}
library(bit64)

# Get the edition info from the folder names
release_version<-str_sub(getwd(), start = -9)
ed <- str_sub(getwd(), start = -2) # eg 06
edition <- release_version  # eg edition06
prev_ed <- get_previous_ed(ed)


# Create new folders ------------------------------------------------------

# Define folders
data_folder <- paste0("your/folder/here", edition)

intermediary_folder <- paste0(data_folder, "/intermediary_rdata/")
hh_sums_folder <- paste0(intermediary_folder, "hh_sums/")
hh_error_summaries_folder <- paste0(intermediary_folder, "hh_error_summaries/")

final_csv_folder <- paste0(data_folder, "/processed_csv/")
final_hh_csv_folder <- paste0(final_csv_folder, "serl_smart_meter_hh_", release_version, "/")
final_climate_folder <- paste0(final_csv_folder, "serl_climate_data_", release_version, "/")


final_rdata_folder <- paste0(data_folder, "/processed_rdata/")
final_hh_rds_folder <- paste0(final_rdata_folder, "serl_smart_meter_hh_", release_version, "/")

raw_data_folder <- paste0(data_folder, "/raw/")
raw_hh_folder <- paste0(raw_data_folder, "Half-hourly/")
raw_daily_folder <- paste0(raw_data_folder, "Daily/")
raw_climate_folder <- paste0(raw_data_folder, "Climate/")
raw_epc_folder <- paste0(raw_data_folder, "SERL_EPC/")

prev_hh_data_folder <- paste0(data_folder, "/reuse_from_prev_ed/hh_data_csv/")
prev_hh_sums_folder <- paste0(data_folder, "/reuse_from_prev_ed/hh_sums_rds/")
prev_hh_errors_folder <- paste0(data_folder, "/reuse_from_prev_ed/hh_error_summaries_rds/")
prev_hh_rds_folder <- paste0(paste0(substr(raw_data_folder, 1, 
                                           nchar(raw_data_folder) - 7)),
                                    prev_ed, 
                                    "/processed_rdata/serl_smart_meter_hh_edition",
                                    prev_ed,
                                    "/")
prev_climate_folder <- get_prev_climate_folder(ed)# this is not used in monthly processing

folder_for_daily_csv <- paste0(final_csv_folder, 
                              "serl_smart_meter_daily_",
                              release_version, "/")
folder_for_hh_rdata = paste0(final_rdata_folder,
                             "serl_smart_meter_hh_", release_version, "/")


prev_raw_data_folder <- paste0(substr(raw_data_folder, 1, nchar(raw_data_folder) - 7),prev_ed,"/raw/")
prev_raw_data_files <- list.files(prev_raw_data_folder)
prev_full_erasures_filename <- prev_raw_data_files[str_detect(prev_raw_data_files, "rasure")]
prev_erasure_data_file <- paste0(prev_raw_data_folder, prev_full_erasures_filename)

markdown_folder <- paste0(getwd(), "/documentation/")
word_doc_folder <- paste0("your/folder/here",release_version,"/", sep="")
final_pdf_folder <- paste0("your/folder/here" , release_version, "/")


# Check they all exist, create them if not, report how many in the console
# Also copies the word doc templates from the previous edition. Note: you need to have them in your
# personal previous edition documentation folder for this to work
create_missing_folders(data_folder,
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
                       raw_epc_folder,
                       prev_hh_data_folder,
                       prev_hh_sums_folder,
                       prev_hh_errors_folder,
                       markdown_folder,
                       word_doc_folder,
                       final_pdf_folder,
                       prev_ed)

# If this has not already happened, copy required raw data etc. from \Data-extracts

# this code should only be run once up to date erasures file has been copied to raw data folder
raw_data_files <- list.files(raw_data_folder)
full_erasures_filename <- raw_data_files[str_detect(raw_data_files, "rasure")]
erasure_data_file <- paste0(raw_data_folder, full_erasures_filename)


# Smart meter data processing and documentation ---------------------------
# add data to editionXX/data/raw and editionXX/data/reuse_from_prev_ed as required
# Check all the required files are where they need to be
source("./smart_meter_setup.R")
# perform full erasure for new half-hourly and daily smart meter data and sm input files
perform_full_erasures_smart_meter_input_files(actual_starts_file,
                                              inventory_file,
                                              participant_details_file,
                                              daily_filepaths,
                                              hh_filepaths,
                                              erasure_data_file)

# Process the 'old' half-hourly smart meter files (full erasure and renaming)
# inputs need updating
update_previous_hh_files(prev_hh_data_folder,
                         prev_hh_sums_folder,
                         prev_hh_errors_folder,
                         prev_hh_rds_folder,
                         final_hh_csv_folder,
                         final_hh_rds_folder,
                         hh_sums_folder,
                         hh_error_summaries_folder,
                         erasure_data_file,
                         prev_erasure_data_file,
                         release_version)

# Process the new half-hourly smart meter files (can be done before processing the old ones if preferred)
create_new_monthly_hh_files(most_recent_smart_meter_date,
                            inventory_file,
                            participant_details_file,
                            actual_starts_file,
                            bst_dates_file,
                            hh_folder,
                            hh_filenames,
                            final_hh_csv_folder,
                            final_hh_rds_folder,
                            hh_sums_folder,
                            hh_error_summaries_folder,
                            release_version)

# 3.5 hours for 51 files

# For the daily data no issues rerunning everything as the files are so much smaller
system.time(create_new_daily_files(raw_data_folder,
                       hh_sums_folder,
                       most_recent_smart_meter_date,
                       folder_for_daily_csv,
                       intermediary_folder,
                       release_version)
)/60
# 8.2 mins up to end of June 2023

system.time(create_read_type_summary_file(intermediary_folder,
                              hh_sums_folder,
                              hh_error_summaries_folder,
                              final_csv_folder,
                              final_rdata_folder, 
                              release_version)
)/60
# don't worry about the warnings. If there isn't any of a 
# particular error for a participant then it's upset because it has to count nothing, or
# if there are no valid reads then it doesn't like finding the smallest / largest value

system.time(create_variables_for_smart_meter_documentation(final_rdata_folder,
                                               hh_sums_folder,
                                               release_version,
                                               folder_for_hh_rdata,
                                               intermediary_folder)
)/60
# 0.37 mins

# Create the smart meter documentation
rmarkdown::render(input = paste0(markdown_folder, "serl_smart_meter_documentation.Rmd"),
                  output_format = "word_document",
                  output_file = paste0(word_doc_folder,
                                       "serl_smart_meter_documentation_",
                                       release_version,
                                       ".docx"))

# Create the smart meter data quality report
rmarkdown::render(input = paste0(markdown_folder, "serl_smart_meter_data_quality_report.Rmd"),
                  output_format = "word_document",
                  output_file = paste0(word_doc_folder,
                                       "serl_smart_meter_data_quality_report_",
                                       release_version,
                                       ".docx"))

# EPC data processing and documentation --------------------------------------------------

# Perform full erasure on EPC input data
# No previous dependencies. Requires England and Scotland data and automatically calls
#   Previous edition raw data for comparison. Must be run before create.participant.summary()
#   and before creating the EPC documentation. 
# Includes full erasure
create_epc_file(raw_data_folder,
                intermediary_folder,
                prev_raw_data_folder,
                erasure_data_file,
                release_version,
                stop_if_column_name_changes = TRUE)

# create EPC documentation
rmarkdown::render(input = paste0(markdown_folder, "serl_epc_documentation.Rmd"),
                  output_format = "word_document",
                  output_file = paste0(word_doc_folder,
                                       "serl_epc_documentation_",
                                       release_version,
                                       ".docx"))

# Climate data processing and documentation -------------------------------

process_all_climate_files(prev_climate_folder,
                          final_climate_folder,
                          raw_climate_folder,
                          release_version) 

# All survey data processing and documentation ----------------------------

# Update (rename and full erasures) previously-processed survey data.
#  Add any new survey files to the inputs
#  Files picked up from the previous edition processed_csv folder automatically
#  survey_data_filename_stubs have PUPRNs (ie need full erasures)
#  data_dictionary_filename_stubs don't have PUPRNs (only need renaming)
#  full erasure file is the only new file required to run this function

update_survey_data( c("serl_covid19_survey_data","serl_2023_follow_up_survey_data",
                      "serl_survey_data"),
                    c("serl_covid19_survey_data_dictionary","serl_2023_follow_up_survey_data_dictionary",
                      "serl_survey_data_dictionary"),
                    final_csv_folder,
                    final_pdf_folder,
                    release_version, #changed from ed 16/1/24
                    erasure_data_file)


#Survey, climate and participant information documentation is now contained in 'static' word documents

# Generate participant summary data file ---------------------------------------------
# Requires EPC, survey, rt_summary
create_participant_summary(final_rdata_folder,
                           release_version,
                           survey_file, 
                           participant_details_file, 
                           epc_file,
                           final_csv_folder)


# Readme document ---------------------------------------------------------

rmarkdown::render(input = paste0(markdown_folder, "serl_readme_data_and_documentation_summary.Rmd"),
                  output_format = "word_document",
                  output_file = paste0(word_doc_folder,
                                       "serl_readme_data_and_documentation_summary_",
                                       release_version, # should use release version
                                       ".docx"))

# Finally manually edit the word documents to move the file info above the table
# Tariff code runs separately in python
