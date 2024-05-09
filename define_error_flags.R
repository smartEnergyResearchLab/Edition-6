# Error codes -------------------------------------------------------------

# Error flags:
#   3   No read but invalid read time for another read type (ie ignore, not an issue)
#   2   No meter - or at least none we find via the DCC inventory
#   1   Valid read - a read that doesn't meet any other error flagging criteria
#   0   Missing read for an existing meter
#  -1  'Max read' - see definition in setup
#  -2  'High read' - see definition in setup
#  -3   Negative read
#  -4   Incorrect units - see 'correctUnits' code chunk
#  -5   Valid read but invalid time
#  -6   Daily electricity active import is zero (strong evidence of errors)
# ---

# minimum number of valid readings required to determine if daily electricity 
#  import data was incorrectly recorded in kWh
min_n_to_determine_unit_error <- 30 

# Define what half-hourly sum and daily read to be matching or similar means
elec_match_limit <- 1     # (Wh)
elec_similar_limit <- 10  # (Wh)
gas_match_limit <- 0.001  # (m^3)
gas_similar_limit <- 0.01 # (m^3)

# We try to make generous limits before classifying a read as dubiously 'high' 
#  200 amp fuse box, with 240 Volt circuit gives 48kW max electricity
#  16m^3/hr is generous for gas flow, therefore 8m^3/half hour
#  4kW is a reasonable maximum for solar PV capacity 

error_codes_hh <- data.table(
  read = c(
    "Elec_act_imp_hh_Wh",
    "Elec_react_imp_hh_varh",
    "Elec_act_exp_hh_Wh",
    "Elec_react_exp_hh_varh",
    "Gas_hh_m3"
  ),
  err_colname = c(
    "Elec_act_imp_flag",
    "Elec_react_imp_flag",
    "Elec_act_exp_flag",
    "Elec_react_exp_flag",
    "Gas_flag"
  ),
  max_err = c(16777215,
              16777215,
              16777215,
              16777215,
              16777.215),
  high_err = c(24 * 1000,
               10000,
               10000 / 2,
               10000,
               8),
  deviceType = c(rep("ESME", 4),
                 "GPF"),
  readType = c("AI",
               "RI",
               "AE",
               "RE",
               "AI")
)

error_codes_daily <- data.table(
  read = c("Elec_act_imp_d_Wh",
           "Gas_d_m3"),
  err_colname = c("Elec_act_imp_flag",
                  "Gas_flag"),
  max_err = c(16777215,
              16777.215),
  high_err = c(48 * 24 * 1000,
               8 * 48),
  deviceType = c("ESME",
                 "GPF"),
  readType = c("DL",
               "DL")
)

n_hh_error_types <- nrow(error_codes_hh)
n_daily_error_types <- nrow(error_codes_daily)
n_error_types <- n_hh_error_types + n_daily_error_types
