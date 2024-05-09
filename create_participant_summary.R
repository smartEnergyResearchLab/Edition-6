
create_participant_summary <- function(final_rdata_folder,
                                       release_version,
                                       survey_file, 
                                       participant_details_file, 
                                       epc_file,
                                       final_csv_folder) {
  
  library(data.table)
  library(lubridate)
  library(stringr)
  
  rt_summary_file <- list.files(final_rdata_folder)[str_detect(list.files(final_rdata_folder), "rt_summary")]
  rt_summary <- readRDS(paste0(final_rdata_folder, rt_summary_file))
  participant_details <- fread(participant_details_file)

  
  
  tmp_poss_reads <- copy(rt_summary[, .(PUPRN,
                                        deviceType,
                                        readType,
                                        firstValidReadDate,
                                        lastValidReadDate,
                                        valid,
                                        percValid)])
  setnames(tmp_poss_reads, 
           old = c("firstValidReadDate", "lastValidReadDate", "valid", "percValid"), 
           new = c("Start", "End", "NumValid", "PercValid")
  )
  
  tmp_poss_reads[readType == "AI" & deviceType == "ESME", readType := "HH_Act_Im"]
  tmp_poss_reads[readType == "RI", readType := "HH_React_Im"]
  tmp_poss_reads[readType == "AE", readType := "HH_Act_Ex"]
  tmp_poss_reads[readType == "RE", readType := "HH_React_Ex"]
  tmp_poss_reads[readType == "DL" & deviceType == "ESME", readType := "D_Act_Im"]
  tmp_poss_reads[readType == "DL" & deviceType == "GPF", readType := "D_Im"]
  tmp_poss_reads[readType == "AI" & deviceType == "GPF", readType := "HH_Im"]
  
  ## convert from long to wide 
  participant_summary <- dcast(tmp_poss_reads, PUPRN ~ deviceType + readType,  
                               value.var = c( "Start", "End", "NumValid", "PercValid"))
  
  survey_file <- paste0(final_csv_folder, list.files(final_csv_folder)[str_detect(list.files(final_csv_folder), "serl_survey_data_edition")])
  participant_summary <- attach.survey.data(participant_summary, survey_file)
  participant_details <- fread(participant_details_file)
  participant_summary <- attach.participant.info(participant_summary, participant_details)
  epc_file <- paste0(final_rdata_folder, list.files(final_rdata_folder)[str_detect(list.files(final_rdata_folder), "epc_data")])
  participant_summary <- attach.EPC.basic.info(participant_summary, epc_file)
  
  setnames(participant_summary, 
           old = c("N_answered", 
                   "Perc_answered"),
           new = c("N_survey_ans", 
                   "Perc_survey_ans")
  )
  
  setcolorder(participant_summary, c("PUPRN", 
                                     "Region",
                                     "LSOA",
                                     "grid_cell",
                                     "IMD_quintile",
                                     "EPC_exists",
                                     "EPC_rating",
                                     "N_survey_ans", 
                                     "Perc_survey_ans",
                                     "Start_ESME_D_Act_Im",
                                     "End_ESME_D_Act_Im",
                                     "NumValid_ESME_D_Act_Im",
                                     "PercValid_ESME_D_Act_Im",
                                     "Start_ESME_HH_Act_Im",
                                     "End_ESME_HH_Act_Im",
                                     "NumValid_ESME_HH_Act_Im",
                                     "PercValid_ESME_HH_Act_Im",
                                     "Start_ESME_HH_Act_Ex",
                                     "End_ESME_HH_Act_Ex",
                                     "NumValid_ESME_HH_Act_Ex",
                                     "PercValid_ESME_HH_Act_Ex",
                                     "Start_ESME_HH_React_Im",
                                     "End_ESME_HH_React_Im",
                                     "NumValid_ESME_HH_React_Im",
                                     "PercValid_ESME_HH_React_Im",
                                     "Start_ESME_HH_React_Ex",
                                     "End_ESME_HH_React_Ex",
                                     "NumValid_ESME_HH_React_Ex",
                                     "PercValid_ESME_HH_React_Ex",
                                     "Start_GPF_D_Im",
                                     "End_GPF_D_Im",
                                     "NumValid_GPF_D_Im",
                                     "PercValid_GPF_D_Im",
                                     "Start_GPF_HH_Im",
                                     "End_GPF_HH_Im",
                                     "NumValid_GPF_HH_Im",
                                     "PercValid_GPF_HH_Im")
  )
  
  fwrite(participant_summary,
         file = paste0(final_csv_folder, "serl_participant_summary_", release_version, ".csv"))
  return("Participant summary successfully created")
  
}
