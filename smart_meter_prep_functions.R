# This code contains the functions used by the script
#  serl_smart_meter_data_prep_editionXX.R to prepare the 
#  half-hourly and daily smart meter data for the SERL Observatory datasets.

# Code developed by Ellen Webborn, UCL using R version 4.0.1 (2020-06-06)
# Most recent edits made by Ellen Webborn, 2022-03-22


add.hh.sums <- function(daily, hh_sums) {
  daily <- merge.data.table(
    daily,
    hh_sums[, .(PUPRN,
                Read_date_effective_local,
                Read_date_time_local,
                Elec_act_imp_hh_sum_Wh,
                Gas_hh_sum_m3)],
    by = c("PUPRN", "Read_date_effective_local", "Read_date_time_local"),
    all = TRUE
  )
  return(daily)
}


add.start.end.dates <- function(error_summary, 
                                DT = hh,
                                ecodes = error_codes_hh,
                                n_err_types = n_hh_error_types) {
  
  start.list <- vector(mode = "list", length = n_err_types)
  end.list <- vector(mode = "list", length = n_err_types)
  
    for(i in 1:n_err_types) {
    tmp <- find.first.last.valid.dates(DT, ecodes[i, err_colname],
                                       ecodes[i, deviceType], 
                                       ecodes[i, readType])
    start.list[[i]] <- tmp[[1]]
    end.list[[i]] <- tmp[[2]]
  }
  
  starts_combined <- rbindlist(start.list)
  ends_combined <- rbindlist(end.list)
  key_cols <- c("PUPRN", "deviceType", "readType")
  setkeyv(starts_combined, key_cols)
  setkeyv(ends_combined, key_cols)
  setkeyv(error_summary, key_cols)
  
  error_summary <- starts_combined[error_summary]
  error_summary <- ends_combined[error_summary]
  return(error_summary)
}


attach.EPC.basic.info <- function(participant_summary, epc_file) {
  epc = readRDS(epc_file)
  setkey(epc, PUPRN)
  participant_summary <- epc[, .(PUPRN, currentEnergyRating)][participant_summary]
  participant_summary[, EPC_exists := TRUE]
  participant_summary[is.na(currentEnergyRating), EPC_exists := FALSE]
  setnames(participant_summary, "currentEnergyRating", "EPC_rating")
  return(participant_summary)
}


attach.participant.info <- function(participant_summary, participant_details) {
  setkey(participant_details, PUPRN)
  participant_summary <- participant_details[, .(PUPRN, Region, IMD_quintile, LSOA, 
                                                 grid_cell)][participant_summary]
  return(participant_summary)
}


attach.survey.data <- function(participant_summary, survey_file) {
  survey_new_edition = fread(survey_file)
  setkey(survey_new_edition, PUPRN)
  survey_new_edition[, N_required := 30L + 
                     (A302 > 0 | A303 > 0 | A304 > 0 | A305 > 0 | A306 > 0 |
                        A307 > 0 | A308 > 0 | A309 > 0 | A310 > 0 | A3_Other > 0) +
                     (A402 > 0 |A403 > 0) + 
                     (A6 == 1) + 
                     (A7 == 1) + 
                     (B2 == 2) +
                     (C5 == 1) +
                     (C1 > 1) * 3L
  ]
  
  survey_new_edition[, N_answered := (A1 > 0) + (A2 > -2) + (A3_sum > 0) + (A4_sum > 0) + (A5 > -2) + 
                     (A6 > 0) + (A7 > 0) + (A8 > -2) + (A9_sum > 0) + (A10 > 0) + (A11 > 0) +
                     (A12_Taps_sum > 0) + (A12_Shower_sum > 0) + (A13_01 > 0) + (A13_02 > 0) + 
                     (A14 > -2) + (A1501 > -2) + (A1502 > -2) + (A16_sum > 0) + 
                     (B1 > 0) + (B2 > 0) + (B3 > 0) + (B4 > 0) + (B5> 0) + (B6 > 0) + (B7 > -2) + 
                     (B8 > -2) + (B9 > -2) + (B10_sum > 0) + 
                     (C1 > 0) + (C2_sum > 0) + (C3_sum > 0) + (C4 > -2 & !is.na(C4)) + (C5 > -2) + (C6 > -2) + 
                     (D1 > 0) + (D2 %in% c(-1, 1, 2, 3)) + (D3 > 0 | D3 == -3) + (D4 != -2)
  ]
  
  survey_new_edition[, Perc_answered := round(N_answered / N_required * 100, 2)]
  
  pp_summary <- survey_new_edition[, list(PUPRN, N_answered, Perc_answered)][participant_summary]
  return(pp_summary)
}


calc.error.percentages <- function(rt_summary) {
  rt_summary[, percValid := round(valid / maxPossReads * 100, 2)]
  rt_summary[, percMissing := round(missing / maxPossReads * 100, 2)]
  rt_summary[, percError := round(100 - percValid - percMissing, 2)]
  rt_summary[, percValidOrUnitError := round((valid + wrongUnits) / maxPossReads * 100, 2)] 
}


calc.flag.daily.sum.match <- function(daily, elec_match_limit, elec_similar_limit, 
                                      gas_match_limit, gas_similar_limit) {
  # Calculate difference between daily reads and half-hourly sums
  # Flag if the daily electricity reading matches or is similar to (defined in setup) 
  #  the sum of the half-hourly reads for that day. 
  
  daily[, Elec_act_imp_sum_diff := round(Unit_correct_elec_act_imp_d_Wh - 
                                           Elec_act_imp_hh_sum_Wh, 3)]
  
  daily[, Gas_sum_diff := round(Gas_d_m3 - Gas_hh_sum_m3, 3)]
  
  ## setup all with code 999 in order to catch any types of matching missed
  daily[, Elec_sum_match := 999L]
  
  ## No electricity meter
  daily[Elec_AI_exists == FALSE, 
        Elec_sum_match := 2L] # No meter
  
  ## not possible to compare as don't have 48 valid hh reads and/or a valid daily read
  daily[Elec_sum_match == 999L & is.na(Elec_act_imp_sum_diff), 
        Elec_sum_match := 0L]
  
  ## Meter read recorded in kWh so comparison ignored
  daily[Elec_act_imp_flag == -4L, 
        Elec_sum_match := 3L] 
  
  ## Sum and daily read match (within elec_match_limit)
  daily[Elec_sum_match == 999L & abs(Elec_act_imp_sum_diff) <= elec_match_limit, 
        Elec_sum_match := 1L] 
  
  ## Similar but don't match
  daily[Elec_sum_match == 999L & abs(Elec_act_imp_sum_diff) <= elec_similar_limit,
        Elec_sum_match := -1L] 
  
  ## Don't match and not similar despite all relevant readings being valid
  daily[Elec_sum_match == 999L & abs(Elec_act_imp_sum_diff) > elec_similar_limit, 
        Elec_sum_match := -2]
  
  
  # Flag if the daily gas reading matches or is similar to (defined in setup) 
  #  the sum of the half-hourly reads for that day. 
  
  ## Setup all with code 999 in order to catch any types of matching missed
  daily[, Gas_sum_match := 999L]
  
  ## No gas meter
  daily[Gas_exists == FALSE, 
        Gas_sum_match := 2L] 
  
  ## not possible to compare as don't have 48 valid hh reads and/or a valid daily read
  daily[Gas_sum_match == 999L & is.na(Gas_sum_diff), 
        Gas_sum_match := 0L] 
  
  ## Sum and daily read match (within gas_match_limit defined in setup)
  daily[Gas_sum_match == 999L & abs(Gas_sum_diff) <= gas_match_limit, 
        Gas_sum_match := 1L] 
  
  ## Similar but don't match
  daily[Gas_sum_match == 999L & abs(Gas_sum_diff) <= gas_similar_limit, 
        Gas_sum_match := -1L]
  
  ## Don't match and not similar despite all relevant readings being valid
  daily[Gas_sum_match == 999L & abs(Gas_sum_diff) > gas_similar_limit, 
        Gas_sum_match := -2L]
  
  return(daily)
}


calc.hh.sums.in.months <- function(hh, bst_dates) {
  # Sum the valid reads for each PUPRN for each day
  
  ## Elec import
  hh_elec_sums <- hh[Elec_act_imp_flag == 1,
                     .(Elec_act_imp_hh_sum_Wh = sum(Elec_act_imp_hh_Wh),
                       N_elec_hh = .N),
                     keyby = .(PUPRN, Read_date_effective_local)]
  ## Gas import
  hh_gas_sums <- hh[Gas_flag == 1,
                    .(Gas_hh_sum_m3 = sum(Gas_hh_m3),
                      N_gas_hh = .N),
                    keyby = .(PUPRN, Read_date_effective_local)]
  
  ## Combine
  hh_sums <- merge.data.table(
    hh_elec_sums,
    hh_gas_sums,
    by = c("PUPRN",
           "Read_date_effective_local"),
    all = TRUE
  )
  
  hh_sums[is.na(N_elec_hh), N_elec_hh := 0L]
  hh_sums[is.na(N_gas_hh), N_gas_hh := 0L]
  
  setkey(hh_sums, "Read_date_effective_local")
  hh_sums <- bst_dates[, .(Read_date_effective_local, n_hh)][hh_sums]
  hh_sums[is.na(n_hh), n_hh := 48L]
  
  return(hh_sums)
}


code.errors <- function(sm.data, error_codes) {
  
  for(i in 1:nrow(error_codes)) {
    
    # ESME or GPF doesn't exist/isn't registered/we didn't start data collection yet 
    if(stringr::str_detect(error_codes[i, read], "Elec_act_imp") == TRUE) {
      sm.data[Elec_AI_exists == FALSE, 
              eval(error_codes[i, err_colname]) := 2L]
    } else if(stringr::str_detect(error_codes[i, read], "Elec_react_imp") == TRUE) {
      sm.data[Elec_RI_exists == FALSE, 
              eval(error_codes[i, err_colname]) := 2L]
    } else if(stringr::str_detect(error_codes[i, read], "Gas") == TRUE) {
      sm.data[Gas_exists == FALSE, eval(error_codes[i, err_colname]) := 2L]
    } else {
      sm.data[Elec_exp_exists == FALSE, eval(error_codes[i, err_colname]) := 2L]
    }
    
    # if meter doesn't exist and we see a 0, change to NA
    sm.data[get(error_codes[i, err_colname]) == 2 & 
              get(error_codes[i, read]) == 0,
                  eval(error_codes[i, read]) := NA_integer_]
    
    # if meter exists but no read, code as missing
    sm.data[is.na(get(error_codes[i, read])) & 
              is.na(get(error_codes[i, err_colname])), 
            eval(error_codes[i, err_colname]) := 0L]
    
    # if coded as missing but invalid read time, code as not needed (3)
    sm.data[Valid_read_time == FALSE & 
              get(error_codes[i, err_colname]) == 0,
            eval(error_codes[i, err_colname]) := 3L]
    
    # Now we have meter existence, and actual values, code these values
    
    # Max error
    sm.data[get(error_codes[i, read]) >= error_codes[i, max_err],
            eval(error_codes[i, err_colname]) := -1L]
    
    # High error
    sm.data[get(error_codes[i, read]) >= error_codes[i, high_err] & 
              get(error_codes[i, read]) < error_codes[i, max_err],
            eval(error_codes[i, err_colname]) := -2L]
    
    # Negative
    sm.data[get(error_codes[i, read]) < 0,
            eval(error_codes[i, err_colname]) := -3L]
    
    # Valid (none of the above)
    sm.data[is.na(get(error_codes[i, err_colname])),
            eval(error_codes[i, err_colname]) := 1L]
    
    # Valid but invalid read time
    sm.data[get(error_codes[i, err_colname]) == 1 & 
              Valid_read_time == FALSE,
            eval(error_codes[i, err_colname]) := -5L]
    
    # Code zeros as errors if electricity act imp daily
    if(error_codes[i, read] == "Elec_act_imp_d_Wh") {
      sm.data[get(error_codes[i, err_colname]) == 1 & 
                get(error_codes[i, read]) == 0, 
              eval(error_codes[i, err_colname]) := -6L]
    }
  }
  
  return(sm.data)
}


code.valid.hh.sum.or.daily.read <- function(daily) {
  # adds 2 new columns, TRUE if hh_sums exists or daily read is valid
  
  daily[, Valid_hh_sum_or_daily_elec := (!is.na(Elec_act_imp_hh_sum_Wh) | 
                                           Elec_act_imp_flag == 1)]
  daily[, Valid_hh_sum_or_daily_gas := (!is.na(Gas_hh_sum_m3) | 
                                          Gas_flag == 1)]
  return(daily)
}


convert.gas.daily <- function(daily) {
  daily[, Gas_d_kWh := round(convert.m3.kwh(Gas_d_m3), 3)]
}


convert.gas.hh <- function(hh.data) {
  hh.data[, Gas_hh_kWh := round(convert.m3.kwh(Gas_hh_m3), 3)]
  hh.data[, Gas_hh_Wh := Gas_hh_kWh * 1000]
}


convert.m3.kwh <- function(m3, CV = 39.5) {
  # converts volume of gas in m3 to energy in kWh (estimate)
  kWh <- m3 * 1.02264 * CV / 3.6
}


correct.elec.in.kwh <- function(daily, min_n_to_determine_unit_error) {
  # Correct where all daily reads < 100 and at least 
  #  min_n_to_determine_unit_error reads exist
  max_elec_reads <- daily[Elec_act_imp_flag == 1, max(Elec_act_imp_d_Wh), 
                          keyby = PUPRN]
  id_kWh <- max_elec_reads[V1 < 100, PUPRN]
  n_elec_reads_suspected_kWh <- daily[Elec_act_imp_flag == 1 & 
                                        PUPRN %in% id_kWh, .N, keyby = PUPRN]
  id_kWh_sufficient_N <- n_elec_reads_suspected_kWh[N >= min_n_to_determine_unit_error]
  
  # change error code from valid (1) to unit error (-4)
  daily[PUPRN %in% id_kWh_sufficient_N$PUPRN & 
          Elec_act_imp_flag == 1, 
        Elec_act_imp_flag := -4L]
  
  # create new column for unit-corrected (or original) data
  daily[, Unit_correct_elec_act_imp_d_Wh := Elec_act_imp_d_Wh]
  daily[Elec_act_imp_flag == -4, 
        Unit_correct_elec_act_imp_d_Wh := Elec_act_imp_d_Wh * 1000]
  
  # Correct if daily is approx hh_sum / 1000 (only issue for elec)
  ## create new column to help with comparison. 'Approx' means anything with form 
  #    x000 to x999 (hh sum) and x-1 to x+1 (daily sum)
  daily[, comparator := floor(Elec_act_imp_hh_sum_Wh / 1000)]
  
  # flag anything we haven't caught previously, isn't just a 0 read, and is within our 'approx'
  #  range. Ignoring zeros as lots of one-off cases found that aren't relevant. 
  daily[, might_correct := Unit_correct_elec_act_imp_d_Wh >= comparator - 1 & 
          Unit_correct_elec_act_imp_d_Wh <= comparator + 1 & 
          Unit_correct_elec_act_imp_d_Wh != 0 &
          Elec_act_imp_flag != -4]
  
  ## count how many issues per PUPRN because we'll insist on at least 5 in order to correct
  #  (based on investigations, will miss some and miss-flag others, but seems to minimise issues)
  counts <- daily[might_correct == TRUE, .N, by = PUPRN]
  
  daily[, to_correct := might_correct == TRUE & PUPRN %in% counts[N >= 5, PUPRN]]
  
  # make correction. Note we could have replaced with hh_sums but this stays consistent with 
  #  first set of unit correction, and likely other daily reads will want to be replaced with
  #  hh_sums by researchers
  daily[to_correct == TRUE, `:=`(Elec_act_imp_flag = -4L,
                                 Unit_correct_elec_act_imp_d_Wh = Elec_act_imp_d_Wh * 1000)]
  
  # delete columns no longer required
  daily[, `:=`(comparator = NULL,
               might_correct = NULL,
               to_correct = NULL)]
  
  return(daily)
}


correct.theoretical.start <- function(rt_summary) {
  # sometimes a meter gets replaced and we didn't know about the previous one
  #  therefore we get readings sooner than we expected
  #  current approach is to adjust the theoretical start date
  
  rt_summary[firstValidReadDate < theoreticalStart, 
             `:=`(theoreticalStart = firstValidReadDate,
                  daysRange = as.integer(theoreticalEnd - theoreticalStart) + 1L,
                  maxPossReads = daysRange * 48L)]
  rt_summary[readType == "DL", maxPossReads := daysRange]
  return(rt_summary)
}



fill.read.type.existence <- function(dType, rType, existsCol,
                                     sm = sm_data, 
                                     rd = readDates,
                                     commissioned_gsme = commissioned_gsme) {
  sm <- rd[deviceType == dType & 
             readType == rType, 1:5][sm]
  
  if(rType == "DL") {
    date_colname <- "Read_date_effective_local"
  } else {
    date_colname <- "Read_date_effective"
  }
  
  sm[is.na(readType) | 
       theoreticalStart > get(date_colname) |
       theoreticalEnd < get(date_colname), 
     eval(existsCol) := FALSE]
 
  sm[, `:=`(deviceType = NULL,
            readType = NULL,
            theoreticalStart = NULL,
            theoreticalEnd = NULL)]
  
  # Extra check for gas as GPF-only isn't valid, requires commissioned GSME (rare issue)
  if(dType == "GPF") {
    # commissioned_gsme <- inventory[deviceType == "GSME" & 
    #                                  deviceStatus == "Commissioned", 
    #                                PUPRN]
    sm[get(existsCol) == TRUE & 
         !(PUPRN %in% commissioned_gsme), eval(existsCol) := FALSE]
  }
  return(sm)
}


find.first.last.valid.dates <- function(dt, error_flag, dType, rType) {
  valid_start_dates <- dt[get(error_flag) == 1 & Valid_read_time == TRUE, 
                          min(Read_date_effective_local, na.rm = TRUE), 
                          keyby = PUPRN]
  setnames(valid_start_dates, "V1", "firstValidReadDate")
  
  valid_end_dates <- dt[get(error_flag) == 1 & Valid_read_time == TRUE, 
                        max(Read_date_effective_local, na.rm = TRUE), 
                        keyby = PUPRN]
  setnames(valid_end_dates, "V1", "lastValidReadDate")
  
  valid_start_dates[, `:=`(deviceType = dType,
                           readType = rType)]
  
  valid_end_dates[, `:=`(deviceType = dType,
                         readType = rType)]
  
  return(list(valid_start_dates, valid_end_dates))
}


format.daily.date.times <- function(daily) {
  
  # Configure read_date_time, make clear it's in local time not UTC
  setnames(daily, "Read_date_time", "Read_date_time_local")
  daily[, Read_date_time_local := lubridate::force_tz(Read_date_time_local,
                                                      "Europe/London")]
  
  # Set the effective read date (day before if at midnight (ideal), or < midday)
  daily[, Read_date_effective_local := date(Read_date_time_local)]
  daily[hour(Read_date_time_local) < 12, 
        Read_date_effective_local := Read_date_effective_local - 1]
  
  # remove unnecessary columns
  daily[, `:=`(Read_date_time_effective = NULL,
               Read_date_effective = NULL,
               Valid_24h_read_flag = NULL)]
  return(daily)
}


format.hh.date.times <- function(hh) {
  hh[, `:=`(Read_date_time_effective = NULL,
            Read_date_effective = NULL)]
  
  tmp <- data.table(Read_date_time_UTC = unique(hh$Read_date_time_UTC))
  tmp[, Read_date_time_local := format(Read_date_time_UTC, 
                                       tz = "Europe/London",
                                       usetz = TRUE)]
  tmp[, Read_date_effective_local := date(Read_date_time_local)]
  tmp[hour(Read_date_time_local) == 0 & 
        minute(Read_date_time_local) < 15, 
      Read_date_effective_local := Read_date_effective_local - 1]
  setkey(tmp, "Read_date_time_UTC")
  setkey(hh, "Read_date_time_UTC")
  
  hh <- tmp[hh]
  return(hh)
}


get.daily.error.summary <- function(daily = daily,
                                    error_codes_daily = error_codes_daily,
                                    n_daily_error_types = n_daily_error_types) {
  
  daily_err_summary <- get.daily.read.type.info(daily,
                                                error_codes_daily,
                                                n_daily_error_types)
  daily_err_summary <- get.sm.stats(daily_err_summary, 
                                    daily,
                                    error_codes_daily)
  daily_err_summary <- add.start.end.dates(daily_err_summary,
                                           daily,
                                           error_codes_daily,
                                           n_daily_error_types)
}


get.daily.read.type.info <- function(daily = daily,
                                     error_codes_daily = error_codes_daily,
                                     n_daily_error_types = n_daily_error_types) {
  
  tmp.list <- lapply(1:n_daily_error_types, function(i) {
    return(sum.errors(dt = daily,
                      error_colname = error_codes_daily[i, err_colname],
                      dType = error_codes_daily[i, deviceType],
                      rType = error_codes_daily[i, readType]))
  })
  
  error_summary <- rbindlist(tmp.list, fill=TRUE)
  setkeyv(error_summary, c("PUPRN", "deviceType", "readType"))
  
  # remove rows where the meter doesn't exist
  if("2" %in% colnames(error_summary)) {
    error_summary <- error_summary[is.na(`2`)]
    # remove columns we don't need
    error_summary[, `2` := NULL]
  }
  
  if("3" %in% colnames(error_summary)) {
    error_summary[, `3` := NULL]
  }
  
  error_summary <- replace.na.in.data.table(error_summary)
  err_count_colnames <- c("suspiciousZero",
                          "validWrongTime", 
                          "wrongUnits", 
                          "negative", 
                          "highRead", 
                          "maxRead", 
                          "missing", 
                          "valid")
  setnames(error_summary, 
           old = as.character(-6:1), 
           new = err_count_colnames, 
           skip_absent = TRUE)
  
  err_cols <- colnames(error_summary)
  for(i in 1:length(err_count_colnames)) {
    if(!(err_count_colnames[i] %in% err_cols)) {
      error_summary[, eval(err_count_colnames[i]) := 0]
    }
  }
  
  return(error_summary)
}


get.decommission.dates <- function(inventory) {
  # for each PUPRN-deviceType, check if there are multiple entries (device replacement)
  # create table of decommission dates for each PUPRN-deviceType
  
  inventory[dateDecommissioned == "", dateDecommissioned := NA_character_]
  multi_entries <- inventory[, .N, keyby = list(PUPRN, deviceType)][N > 1]
  n_multi <- nrow(multi_entries)
  multi_resolved <- data.table(PUPRN = multi_entries$PUPRN,
                               deviceType = multi_entries$deviceType,
                               dateDecommissioned = rep(NA_character_, n_multi))
  for (i in 1:n_multi) {
    multi_resolved[i, dateDecommissioned := inventory[PUPRN == multi_resolved[i, PUPRN] &
                                                        deviceType == multi_resolved[i, deviceType],
                                                      max(dateDecommissioned)]]
  }
  
  multi_resolved <- rbind(multi_resolved, 
                          inventory[!PUPRN %in% multi_entries$PUPRN, 
                                    .(PUPRN, deviceType, dateDecommissioned)])
  multi_resolved[, dateDecommissioned := as.Date(dateDecommissioned,
                                                 format = "%Y/%m/%d")]
  return(multi_resolved)
}


get.hh.monthly.read.type.info <- function(hh) {
  
  tmp.list <- lapply(1:n_hh_error_types, function(i) {
    return(sum.errors(dt = hh,
                      error_colname = error_codes_hh[i, err_colname],
                      dType = error_codes_hh[i, deviceType],
                      rType = error_codes_hh[i, readType]))
  })
  
  error_summary <- rbindlist(tmp.list, fill=TRUE)
  setkeyv(error_summary, c("PUPRN", "deviceType", "readType"))
  
  # remove rows where the meter doesn't exist
  if("2" %in% colnames(error_summary)) {
    error_summary <- error_summary[is.na(`2`)]
    # remove columns we don't need
    error_summary[, `2` := NULL]
  }
  
  if("3" %in% colnames(error_summary)) {
    error_summary[, `3` := NULL]
  }
  
  error_summary <- replace.na.in.data.table(error_summary)
  err_count_colnames <- c("suspiciousZero",
                          "validWrongTime", 
                          "wrongUnits", 
                          "negative", 
                          "highRead", 
                          "maxRead", 
                          "missing", 
                          "valid")
  setnames(error_summary, 
           old = as.character(-6:1), 
           new = err_count_colnames, 
           skip_absent = TRUE)
  
  err_cols <- colnames(error_summary)
  for(i in 1:length(err_count_colnames)) {
    if(!(err_count_colnames[i] %in% err_cols)) {
      error_summary[, eval(err_count_colnames[i]) := 0]
    }
  }
  
  return(error_summary)
}


get.hh.sums.valid.n <- function(daily) {
  Elec_sums_or_daily_valid <- daily[Valid_hh_sum_or_daily_elec == TRUE, 
                                    .(validOrHHsumValid = .N,
                                      deviceType = "ESME",
                                      readType = "DL"), 
                                    keyby = PUPRN]
  
  Gas_sums_or_daily_valid <- daily[Valid_hh_sum_or_daily_gas == TRUE, 
                                   .(validOrHHsumValid = .N,
                                     deviceType = "GPF",
                                     readType = "DL"), 
                                   keyby = PUPRN]
  
  sums_or_daily_valid <- rbind(Elec_sums_or_daily_valid,
                               Gas_sums_or_daily_valid)
  setkeyv(sums_or_daily_valid, c("PUPRN", "deviceType", "readType"))
  
  return(sums_or_daily_valid)
}


get.meter.existence <- function(sm_data, 
                                sm_starts, 
                                commissioned_gsme,
                                inv = inventory, 
                                resolution = "hh") {
  # Note: previously assumed that if half-hourly exists then daily exists
  # but SMETS1 don't have daily. Will record as not existing, but add in hh_sums
  
  setkey(sm_starts, "PUPRN")
  setkey(sm_data, "PUPRN")
  
  if (resolution == "hh") {
    
    sm_data[, `:=`(Elec_AI_exists = TRUE,
                   Elec_RI_exists = TRUE,
                   Elec_exp_exists = TRUE,
                   Gas_exists = TRUE)]
    
    sm_data <- fill.read.type.existence("ESME", "AI", "Elec_AI_exists", 
                                        sm_data, sm_starts, commissioned_gsme)
    sm_data <- fill.read.type.existence("ESME", "RI", "Elec_RI_exists", 
                                        sm_data, sm_starts, commissioned_gsme)
    sm_data <- fill.read.type.existence("ESME", "AE", "Elec_exp_exists", 
                                        sm_data, sm_starts, commissioned_gsme)
    sm_data <- fill.read.type.existence("GPF", "AI", "Gas_exists", 
                                        sm_data, sm_starts, commissioned_gsme)
  } else {
    # Daily
    sm_data[, `:=`(Elec_AI_exists = TRUE,
                   Gas_exists = TRUE)]
    sm_data <- fill.read.type.existence("ESME", "DL", "Elec_AI_exists", 
                                        sm_data, sm_starts, commissioned_gsme)
    sm_data <- fill.read.type.existence("GPF", "DL", "Gas_exists", 
                                        sm_data, sm_starts, commissioned_gsme)
  }
  return(sm_data)
}


get.sm.stats <- function(error_summary, dt, ecodes = error_codes_hh) {
  
  tmp1 <- lapply(1:nrow(ecodes), function(i) {
    return(dt[get(ecodes[i, err_colname]) == 1 & 
                Valid_read_time == TRUE, 
              .(minValidRead = as.double(min(get(ecodes[i, read]), na.rm = TRUE)),
                maxValidRead = as.double(max(get(ecodes[i, read]), na.rm = TRUE)),
                sumValidRead = as.double(sum(get(ecodes[i, read]), na.rm = TRUE), 2),
                deviceType = ecodes[i, deviceType],
                readType = ecodes[i, readType]
              ),
              keyby = PUPRN])
    
  })
  
  read_stats <- rbindlist(tmp1, fill=TRUE)
  setkeyv(read_stats, c("PUPRN", "deviceType", "readType"))
  error_summary <- read_stats[error_summary]
  return(error_summary)
}


name.daily.cols <- function(daily_data) {
  setnames(daily_data, 
           old = c("Elec_current_read",
                   "Gas_current_read"),
           new = c("Elec_act_imp_d_Wh",
                   "Gas_d_m3")
  )
  return(daily_data)
}


name.hh.cols <- function(hh_data) {
  setnames(hh_data, 
           old = c("HH_segment", 
                   "elec_active_import_profile_hh_wh",
                   "elec_reactive_import_profile_hh_wh",
                   "elec_active_export_profile_hh_wh",
                   "elec_reactive_export_profile_hh_wh",
                   "gas_active_import_profile_hh_L",
                   "Read_date_time"),
           new = c("HH",
                   "Elec_act_imp_hh_Wh",
                   "Elec_react_imp_hh_varh",
                   "Elec_act_exp_hh_Wh",
                   "Elec_react_exp_hh_varh",
                   "Gas_hh_m3",
                   "Read_date_time_UTC")
  )
  return(hh_data)
}


process.each.hh.month.individually <- function(filenumber,
                                               final_csv_folder,
                                               final_rdata_folder,
                                               hh_sums_folder,
                                               hh_error_summaries_folder,
                                               inventory,
                                               readDates,
                                               bst_dates,
                                               commissioned_gsme,
                                               release_version) {
  # Creates monthly half-hourly files, saves if required
  # outputs half-hourly sums

  #source(paste(getwd(), "/Code/add_packages_to_path.R", sep = ""))
  library(data.table)
  library(lubridate)
  library(stringr)
  
  
  source("./functions/smart_meter_prep_functions.R") 
  
  hh_filename <- hh_filenames[filenumber]
  hh <- fread(paste(hh_folder, hh_filename, sep = ""))

  hh[elec_active_import_profile_hh_wh > 16777215, 
     elec_active_import_profile_hh_wh := 16777215]
  hh[, elec_active_import_profile_hh_wh := as.integer(elec_active_import_profile_hh_wh)]
  
  name.hh.cols(hh)
  hh <- get.meter.existence(sm_data = hh, 
                            sm_starts = readDates,
                            commissioned_gsme = commissioned_gsme)
  
  hh <- format.hh.date.times(hh)
  
  hh[, Valid_read_time := HH %in% seq(1:48)]
  
  code.errors(hh, error_codes_hh)
  
  convert.gas.hh(hh)
  
  select.hh.cols(hh)
  
  # Save half-hourly data
  saveRDS(hh, 
          file = paste0(final_rdata_folder,
                        tolower(substr(hh_filename, 1, nchar(hh_filename) - 3)),
                        "RDS"))
  fwrite(hh,
         file = paste0(final_csv_folder,
                       tolower(substr(hh_filename, 1, nchar(hh_filename) - 4)),
                       "_", release_version, ".csv"))
  
  # Save half-hourly sums
  hh_sums <- calc.hh.sums.in.months(hh, bst_dates) 
  saveRDS(hh_sums,
          file = paste0(hh_sums_folder,
                        "hh_sums_",
                        tolower(substr(hh_filename, 18, nchar(hh_filename) - 3)),
                        "RDS"))
  
  # Save half-hourly error summary
  error_summary <- get.hh.monthly.read.type.info(hh)
  error_summary <- get.sm.stats(error_summary, hh)
  error_summary <- add.start.end.dates(error_summary, hh)
  saveRDS(error_summary,
          file = paste0(hh_error_summaries_folder,
                        "hh_error_summary_",
                        tolower(substr(hh_filename, 18, nchar(hh_filename) - 3)),
                        "RDS"))
  
}


process.hh.sums.combined <- function(hh_sums) {
  hh_sums <- rbindlist(hh_sums, use.names = TRUE)
  
  # 1st day of the month in local time (needed for daily reads) overlaps 2 months 
  # in UTC time, so we need to add them together from the different hh files
  hh_sums <- hh_sums[, .(N_elec_hh = sum(N_elec_hh, na.rm = TRUE),
                         Elec_act_imp_hh_sum_Wh = sum(Elec_act_imp_hh_sum_Wh, na.rm = TRUE),
                         N_gas_hh = sum(N_gas_hh, na.rm = TRUE),
                         Gas_hh_sum_m3 = sum(Gas_hh_sum_m3, na.rm = TRUE)),
                     keyby = .(Read_date_effective_local, PUPRN, n_hh)]
  
  hh_sums <- hh_sums[N_elec_hh == n_hh | N_gas_hh == n_hh]
  hh_sums[N_elec_hh != n_hh, Elec_act_imp_hh_sum_Wh := NA]
  hh_sums[N_gas_hh != n_hh, Gas_hh_sum_m3 := NA]

  # adding for 4th edition to merge on date times with daily
  hh_sums[, Read_date_time_local := lubridate::force_tz(Read_date_effective_local + 1,
                                                             "Europe/London")]
  return(hh_sums)
}


reorder.rt_summary.cols <- function(rt_summary) {
  data.table::setcolorder(rt_summary, 
                          neworder = c("PUPRN", 
                                       "deviceType", 
                                       "readType", 
                                       "theoreticalStart", 
                                       "theoreticalEnd",
                                       "firstValidReadDate",
                                       "lastValidReadDate",
                                       "daysRange",
                                       "maxPossReads", 
                                       "percValid", 
                                       "percValidOrUnitError", 
                                       "percMissing", 
                                       "percError", 
                                       "valid", 
                                       "validOrHHsumValid",
                                       "validWrongTime",
                                       "wrongUnits", 
                                       "suspiciousZero",
                                       "missing", 
                                       "maxRead", 
                                       "highRead", 
                                       "negative", 
                                       "minValidRead",
                                       "maxValidRead",
                                       "meanValidRead")
  )
}


replace.na.in.data.table <- function(dt, replacement = 0) {
  for(i in names(dt)) {
    dt[is.na(get(i)), (i) := replacement]
  }
  return(dt)
}


save.yearly.daily.files <- function(daily) {
  daily[, y := year(Read_date_effective_local)]
  
  daily_years <- split(daily, by = "y", sorted = TRUE)
  
  # remove y column and save csv
  lapply(1:length(daily_years), function(x) {
    daily_saving_name <- paste("/serl_smart_meter_daily_", 
                               daily_years[[x]][1, y], "_", 
                               release_version, ".csv", sep = "")
    daily_years[[x]][, y := NULL]
    fwrite(daily_years[[x]], file = paste(location_processed, 
                                          get.serl.filename("daily_data", release_version),
                                          daily_saving_name,
                                          sep = ""))
    
  })
  return(NULL)
}


select.daily.cols <- function(daily) {
  # remove unwanted columns, order columns, sort rows, add UTC column
  
  daily[, Read_date_time_UTC := format(Read_date_time_local, 
                                       tz = "UTC",
                                       usetz = FALSE)]
  
  daily[, `:=`(Elec_AI_exists = NULL,
               Gas_exists = NULL,
               Elec_act_imp_sum_diff = NULL,
               Gas_sum_diff = NULL,
               Read_date_time_local = NULL)]
  
  setcolorder(
    daily,
    c("PUPRN",
      "Read_date_effective_local",
      "Read_date_time_UTC",
      "Valid_read_time",
      "Elec_act_imp_flag",
      "Valid_hh_sum_or_daily_elec",
      "Elec_sum_match",
      "Gas_flag",
      "Valid_hh_sum_or_daily_gas",
      "Gas_sum_match",
      "Elec_act_imp_d_Wh",
      "Unit_correct_elec_act_imp_d_Wh",
      "Elec_act_imp_hh_sum_Wh",
      "Gas_d_m3",
      "Gas_hh_sum_m3",
      "Gas_d_kWh")
  )
  
  setkeyv(daily, c("PUPRN", "Read_date_effective_local"))
  
  return(daily)
}


select.hh.cols <- function(hh) {
  # remove unwanted columns, order columns, sort rows
  hh[, `:=`(Elec_AI_exists = NULL,
            Elec_RI_exists = NULL,
            Elec_exp_exists = NULL,
            Gas_exists = NULL,
            Gas_hh_kWh = NULL)]
  
  setcolorder(
    hh,
    c("PUPRN", 
      "Read_date_effective_local",
      "Read_date_time_local",
      "Read_date_time_UTC",
      "HH",
      "Valid_read_time",
      "Elec_act_imp_flag",
      "Elec_react_imp_flag",
      "Elec_act_exp_flag",
      "Elec_react_exp_flag",
      "Gas_flag",
      "Elec_act_imp_hh_Wh",
      "Elec_react_imp_hh_varh",
      "Elec_act_exp_hh_Wh",
      "Elec_react_exp_hh_varh",
      "Gas_hh_m3",
      "Gas_hh_Wh"))
  
  setkeyv(hh, c("PUPRN", "Read_date_time_UTC"))
  
  return(hh)
}


setup.rt.summary <- function(rt_summary, daily_error_summary, readDates) {
  rt_summary <- rbind(rt_summary, daily_error_summary)
  
  rt_summary <- rt_summary[, .(firstValidReadDate = min(firstValidReadDate, na.rm = TRUE),
                               lastValidReadDate = max(lastValidReadDate, na.rm = TRUE),
                               valid = sum(valid, na.rm = TRUE),
                               validWrongTime = sum(validWrongTime, na.rm = TRUE),
                               suspiciousZero = sum(suspiciousZero, na.rm = TRUE),
                               wrongUnits = sum(wrongUnits, na.rm = TRUE),
                               missing = sum(missing, na.rm = TRUE),
                               maxRead = sum(maxRead, na.rm = TRUE),
                               highRead = sum(highRead, na.rm = TRUE),
                               negative = sum(negative, na.rm = TRUE),
                               minValidRead = min(minValidRead, na.rm = TRUE),
                               maxValidRead = max(maxValidRead, na.rm = TRUE),
                               meanValidRead = round(sum(sumValidRead) / sum(valid), 3)),
                           by = .(PUPRN, deviceType, readType)]
  
  
  setkeyv(rt_summary, c("PUPRN", "deviceType", "readType"))
  setkeyv(readDates, c("PUPRN", "deviceType", "readType"))
  rt_summary <- rt_summary[readDates]
  
  for(i in c("valid", 
             "validWrongTime", 
             "suspiciousZero",
             "wrongUnits", 
             "missing", 
             "maxRead", 
             "highRead",
             "negative")) {
    rt_summary[is.na(get(i)), (i) := 0]
  }
  
  # count actual missing
  rt_summary[, missing := maxPossReads - valid - validWrongTime - 
               suspiciousZero - wrongUnits - maxRead - highRead - negative]
  
  return(rt_summary)
}


sum.errors <- function(dt = hh, 
                       error_colname = "Elec_act_imp_flag", 
                       dType = "ESME", 
                       rType = "AI") {

  dt2 <- dt[, .N, keyby = .(PUPRN, get(error_colname))]
  dt3 <- dcast(dt2, PUPRN ~ get, value.var = "N")
  dt3[, `:=`(deviceType = dType,
             readType = rType)]
  return(dt3)
}


validate.daily.read.time <- function(daily) {
  daily[, Valid_read_time := TRUE]
  daily[hour(Read_date_time_local) != 0 | 
          minute(Read_date_time_local) != 0 |
          second(Read_date_time_local) != 0,
        Valid_read_time := FALSE]
  return(daily)
}
