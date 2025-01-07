# -*- coding: utf-8 -*-
"""
@author: Eoghan McKenna
This script processes raw tariff data for the SERL Observatory and by default
(i.e. when run as a script) it produces a simplified processed tariff data file
 that mirrors the PUPRN and timestamps in the raw data and which  
 researchers may find easier to use than the raw data. Optionally the  
 Tariff_Parser class can be imported from this script so that the tariff object 
 that it creates can be queried to give a tariff value for a given PUPRN and
 timestamp, or the script can be modified to suit other purposes.
"""
#%% Load modules
import pandas as pd
import os
import numpy as np
import re

#%% Specify global paths and options
# Path for location of script
project_path = r'insert_path_here'
# Path and file name for where original tariff data is located
original_tariff_path = r'insert_path_here'
original_tariff_file_name = 'serl_tariff_data_edition06.csv'
# Path and file name for processed simplified tariff data 
results_path = (r'insert_path_here')
results_file_name = 'simplified_serl_tariff_data_edition06.csv'
# Set option for script: 
options = {'produce_csv': True, #<--- Produce a simplied version of the original data
           'display':True} #<--- Print progress to terminal 
#%% Tariff class
class Tariff:
    '''
    Takes (longitudinal) tariff data for a single meter.
    Performs QA and identifies properties of tariff and flags errors
    Has functions which return fixed and standing charge given an input datetime
    '''
    def __init__(self,
                grouped_tariff_df,
                 options={'display':True}
                ):
        # load data and store some basic information about the meter
        self.input_data = grouped_tariff_df.copy()
        self.options = options
        # create a datetime variable to hold the date when the tariff data was fetched
        temp = pd.to_datetime(self.input_data.date, format='%Y%m%d', utc=True).copy()
        if len(temp)==1:
            self.input_data.loc[:,'datetime'] = temp.to_numpy()[0]
        else:
            self.input_data.loc[:,'datetime'] = temp
        # store the PUPRN
        self.PUPRN = self.input_data.PUPRN.unique()[0]
        # store whether this is a gas or electric meter
        meter_string = self.input_data.scheduleType.head(n=1).to_numpy()[0]
        if meter_string == 'GP':
            self.meter = 'gas'
        elif meter_string == 'EP':
            self.meter = 'electricity'
        elif (meter_string != 'GP') & (
            meter_string != 'EP'):
            self.meter = 'unknown'
            if self.options['display']:
                print(f'Error PUPRN {self.PUPRN} unknown schedule type')
        # max value for 32 bit integer
        self.max_32_bit = 2**32 - 1
        self.max_15_bit = 2**15 - 1 
        # set thresholds for large and small values
        self.too_large = 100
        self.too_small = 0.01

    def preprocess_flags(self):
        # this is to flag any errors in the data and facilitate subsequent processing
        self.flags = {}
        # error flag that standing charge scale is all or part missing
        self.flags['bln_scs_all_missing'] = self.input_data.scs.isna().all()
        self.flags['bln_scs_any_missing'] = self.input_data.scs.isna().any()
        self.flags['bln_scs_any_not_missing'] = (~self.input_data.scs.isna()).any()
        if self.flags['bln_scs_any_not_missing']:
            logic = (~self.input_data.scs.isna())
            self.flags['bln_scs_multiple_values'] = len(self.input_data.loc[logic, 'scs'].unique()) > 1
        else:
            self.flags['bln_scs_multiple_values'] = False
        # error flag that primary active charge scale is all or part missing
        self.flags['bln_patps_all_missing'] = self.input_data.patps.isna().all()
        self.flags['bln_patps_any_missing'] = self.input_data.patps.isna().any()
        self.flags['bln_patps_any_not_missing'] = (~self.input_data.patps.isna()).any()
        if self.flags['bln_patps_any_not_missing']:
            logic = (~self.input_data.patps.isna())
            self.flags['bln_patps_multiple_values'] = len(self.input_data.loc[logic, 'patps'].unique()) > 1
        else:
            self.flags['bln_patps_multiple_values'] = False
        # flag patps max error values
        logic = self.input_data.patps == 127
        self.flags['bln_patps_all_max_error'] = logic.all()
        self.flags['bln_patps_any_max_error'] = logic.any()
        
        # flag patp max error value
        logic = self.input_data.patp == self.max_32_bit
        self.flags['bln_patp_all_max_error'] = logic.all()
        self.flags['bln_patp_any_max_error'] = logic.any()

        # error flag that price scale is all or part missing
        self.flags['bln_ps_all_missing'] = self.input_data.ps.isna().all()
        self.flags['bln_ps_any_missing'] = self.input_data.ps.isna().any()
        self.flags['bln_ps_any_not_missing'] = (~self.input_data.ps.isna()).any()
        if self.flags['bln_ps_any_not_missing']:
            logic = (~self.input_data.ps.isna())
            self.flags['bln_ps_multiple_values'] = len(self.input_data.loc[logic, 'ps'].unique()) > 1
        else:
            self.flags['bln_ps_multiple_values'] = False
        
    def preprocess_standing_charge(self):
        # preprocess the standing charge date to enable it to be used in calculations
        self.standing_charge = self.input_data.loc[:, ['datetime','sc','scs']]
        
        # if there are no empty standing charge scale values then calculate the standing charge in GBP, 
        # using the scaling factor 'scs'
        if not self.flags['bln_scs_any_missing']:
            self.standing_charge['sc_GBP'] = self.convert_using_scale(self.standing_charge.sc.to_numpy(),
                                                                      self.standing_charge.scs.to_numpy())
            self.standing_charge['error_code'] = 1
            # otherwise there are some missing values.
        else:
            # if some are missing then assume that scale is determined by length of integer i.e. 22134 is 0.22134 GBP per day
            self.standing_charge['sc_GBP'] = self.convert_using_scale(self.standing_charge.sc.to_numpy(),
                                                                      self.standing_charge.sc.apply(lambda x: -len(str(np.abs(x)))))
            self.standing_charge['error_code'] = 2
            self.standing_charge['error_description'] = 'Missing scale (scale assumed)'
            
        # some times the data is simply not there and sc is zero
        logic = self.standing_charge['sc_GBP'] == 0
        if logic.any():
            self.standing_charge.loc[logic,'error_code'] = -10
            self.standing_charge.loc[logic,'error_description'] = 'Zero value'            
        # a small number of standing charges are negative, which is assumed to be incorrect, so change the sign of these
        logic = self.standing_charge['sc_GBP'] < 0
        temp = -self.standing_charge.loc[logic, 'sc_GBP'].copy()
        self.standing_charge.loc[logic, 'sc_GBP'] = temp
        self.standing_charge.loc[logic,'error_code'] = 3
        self.standing_charge.loc[logic,'error_description'] = 'Negative value (changed sign)'   
        
        # some will have incorrect scales e.g. 21 with a scale of -4, which 
        # will result in incorrectly low values i.e. 0.0021 rather than 0.21
        logic = self.standing_charge['sc_GBP'] < self.too_small
        self.standing_charge.loc[logic,'error_code'] = -3
        self.standing_charge.loc[logic,'error_description'] = f'Value below {self.too_small}'   
        
        logic = self.standing_charge['sc_GBP'] > self.too_large
        self.standing_charge.loc[logic,'error_code'] = -4
        self.standing_charge.loc[logic,'error_description'] = f'Value above {self.too_large}'   
    
    def get_patp_scale(self,
                       input_datetime):
        patps = np.nan
        logic = self.input_data.datetime <= input_datetime
        # if there are any values that match then ... 
        if logic.sum() == 0:
            # no corresponding rows, so return nan (and exit)
            return np.nan
        # otherwise there is a row, so proceed and get it as this is the most recent date we have a record for
        row = self.input_data.loc[logic,:].tail(n=1)
        key = row.iat[0,0]
        logic = row.key == key    
        # check if patps max error
        if self.flags['bln_patps_any_max_error']:
            patps = np.nan
            return patps
        # and now we can now find the scale for this row 
        if not self.flags['bln_patps_any_missing']:
            
            patps = row.loc[logic,'patps'].to_numpy()[0]
            # otherwise there are some missing patp scale values. 
        else:
           
            patps = np.nan
                
        return patps
    
    def get_p_scale(self,
                       input_datetime):
        # get ps
        ps = np.nan
        logic = self.input_data.datetime <= input_datetime
        # if there are any values that match then ... 
        if logic.sum() == 0:
            # no corresponding rows, so return nan (and exit)
            return np.nan
        # otherwise there is a row, so proceed and get it as this is the most recent date we have a record for
        row = self.input_data.loc[logic,:].tail(n=1)
        key = row.iat[0,0]
        logic = row.key == key    
        # and now we can now find the scale for this row 
        if not self.flags['bln_ps_any_missing']:            
            ps = row.loc[logic,'ps'].to_numpy()[0]
            # otherwise there are some missing patp scale values. 
        else:            
            ps = np.nan                
        return ps
    
    def convert_using_scale(self,values,scales):
        # converts an array of prices given a corresponding array of scales to use
        return values/(np.power(10,-scales))
    
    def get_sc(self, input_datetime):
        # given datetime, returns the standing charge(s) for that period
        # find any dates that are equal or before the input datetime
        logic = self.standing_charge.datetime <= input_datetime
        # if there are any values that match then ... 
        if logic.sum() > 0:
            # return the latest entry, as this is the most recent standing charge value we have a record for
            this_sc = self.standing_charge.loc[logic,:].tail(n=1).sc_GBP.to_numpy()[0]
            this_error_code = self.standing_charge.loc[logic,:].tail(n=1).error_code.to_numpy()[0]
            this_error_description = self.standing_charge.loc[logic,:].tail(n=1).error_description.to_numpy()[0]
            return this_sc, this_error_description, this_error_code
        # else return nan
        else:
            this_sc = np.nan
            this_error_code = 0
            this_error_description = 'No data'
            return this_sc, this_error_description, this_error_code
        
    def get_day_profile_index(self,
                              input_datetime):
        # given a datetime, find latest row corresponding to it
        # find any dates that are equal or before the input datetime
        logic = self.input_data.datetime <= input_datetime
        # if there are any values that match then ... 
        if logic.sum() == 0:
            # no corresponding rows, so return nan (and exit)
            return np.nan
        # otherwise there is a row, so proceed and get it as this is the most recent date we have a record for
        row = self.input_data.loc[logic,:].tail(n=1)
        # get the key for this row to enable indexing
        key = row.iat[0,0]
        # how many season profiles?
        # use regex to identify tariff switching table season columns, and record the entries for the matches 
        # set a placeholder for the matches
        season_matches = pd.DataFrame(columns=['reference',
                                               'value',
                                               'season_name',
                                               'season_year',
                                               'season_month',
                                               'season_day',
                                               'season_dayofweek'])
        string_to_match = 'TST_S_' # tariff switching table season
        pattern_string = string_to_match+'\d' # pattern to match is the above string followed by any digit
        p = re.compile(pattern_string)
        for this_col in row.columns:
            m = p.match(this_col) # returns the match
            if m is None:
                # then no match so continue to next iteration of loop
                continue
            else:
                # there is a match, so store the information
                my_dict = {}
                my_dict['reference'] = m.group() # this is the text of the column that matched
                logic = row.key == key
                my_dict['value'] = row.loc[logic, m.group()].to_numpy()[0] # this is the entry for that column
                # if it's empty then continue
                if pd.isna(my_dict['value']):
                    continue
                # otherwise let's parse the season information and store it
                value_split = my_dict['value'].split('_')
                if len(value_split) >1:
                    season_name = value_split[0]
                    season_start_date = value_split[1]
                else:
                    season_name = 'No season name'
                    season_start_date = value_split[0]
                season_split = season_start_date.split('/')
                season_year = season_split[0]
                season_month = season_split[1]
                season_day = season_split[2]
                season_dayofweek = season_split[3]
                my_dict['season_name'] = season_name
                my_dict['season_year'] = season_year
                my_dict['season_month'] = season_month
                my_dict['season_day'] = season_day
                my_dict['season_dayofweek'] = season_dayofweek
                #season_matches = season_matches.append(my_dict, ignore_index=True) #<--- deprecated
                season_matches = pd.concat([season_matches,
                                            pd.DataFrame(my_dict, index=[0])],
                                           ignore_index=True)
        # so now we should have a dataframe with a row for every season profile column, and a value for the corresponding entry
        # if there are any empty values, these will be nan
        number_of_seasons = (~season_matches.value.isna()).sum() # the number of non-nan values is the number of seasons with values
        # if only one season, then determining index of week profile is trivial, it is 1
        if number_of_seasons == 1:
            week_profile_index = 1
        elif number_of_seasons > 1:
            # if more than one season, determine which season
            week_profile_index = self.determine_which_season(input_datetime, season_matches)
            if pd.isna(week_profile_index):
                day_profile_index = np.nan
                error_description = 'Complex season profile. Unable to parse'
                error_code = -6
                tariff_profile_type = np.nan
                return day_profile_index, error_description, error_code, tariff_profile_type
        # otherwise there are cases where season not populated, for gas meter but patp is stored in TTP_1
        elif (season_matches.value.isna()).all() and self.meter == 'gas':
            day_profile_index = -1
            error_description = 'No season data for gas but assumed'
            error_code = 4
            tariff_profile_type = np.nan
            return day_profile_index, error_description, error_code, tariff_profile_type
        else:
            # error determining week profile index
            if self.options['display']:
                print('Error: cannot determine week profile index')
            week_profile_index = np.nan
            day_profile_index = np.nan
            error_description = 'Error: cannot determine week profile index'
            error_code = -7
            tariff_profile_type = np.nan
            return day_profile_index, error_description, error_code, tariff_profile_type
        # now we know the index of the week profile to use, we need to determine the index of the day profile to use
        # fetch the day profile indices for this week profile index
        pattern_string = f'TST_WP_{week_profile_index}_\d' # this is the string pattern to identify all columns corresponding to this week profile index
        p = re.compile(pattern_string)
        week_profile_matches = pd.DataFrame(columns=['reference','value'])
        for this_col in row.columns:
            m = p.match(this_col)
            if m is None:
                continue
            else:
                my_dict = {}
                my_dict['reference'] = m.group()
                logic = row.key == key
                my_dict['value'] = row.loc[logic, m.group()].to_numpy()[0]
                # week_profile_matches = week_profile_matches.append(my_dict, ignore_index=True) # <--- deprecated
                week_profile_matches = pd.concat([week_profile_matches,
                                                  pd.DataFrame(my_dict, index=[0])],
                                                 ignore_index=True)
        # now we have a dataframe with a row for each day profile entry for this week profile index, and a corresponding value which gives the index of the day profile to use
        # if they are all equal, then determining the index of day profile is trivial
        if self.all_equal(week_profile_matches['value']):
            # then day profile index is trivial, take the first value
            day_profile_index = int(week_profile_matches['value'].to_numpy()[0])
            error_description = np.nan
            error_code = 1
            tariff_profile_type = 'Not variable'
        # otherwise check if it looks like a 'weekend' tariff with dp indices 6 and 7 equal and different to the others whihc are also equal
        elif self.all_equal(week_profile_matches.head(n=5).value) and \
            self.all_equal(week_profile_matches.tail(n=2).value):
                
                # in which case the index is determined by the day of the week
                # pandas dayofweek is zero-based (zero is monday) while the day profile index is one-based
                this_dayofweek_index = input_datetime.dayofweek + 1
                logic = week_profile_matches.reference == f'TST_WP_1_{this_dayofweek_index}'
                day_profile_index = week_profile_matches.loc[logic,'value'].to_numpy()[0]
                error_description = np.nan
                error_code = 1
                tariff_profile_type = 'Weekend/weekday'
        else:
            # then there are multiple day profiles for this week, and need to determine which day profile index to use
            # to do that we need to know the date of the start of the season
            # get the relevant info
            logic = season_matches.reference == 'TST_S_'+str(week_profile_index)
            this_season_year = season_matches.loc[logic,'season_year'].to_numpy()[0]
            this_season_month = season_matches.loc[logic,'season_month'].to_numpy()[0]
            this_season_day = season_matches.loc[logic,'season_day'].to_numpy()[0]
            this_season_dayofweek = season_matches.loc[logic,'season_dayofweek'].to_numpy()[0]
            day_profile_index = self.parse_season_date(input_datetime,
                                                      this_season_year,
                                                      this_season_month,
                                                      this_season_day,
                                                      this_season_dayofweek)
            if pd.isna(day_profile_index):
                error_description = 'Error: more than one day profile and unable to determine dp index'
                error_code = -8
                tariff_profile_type = np.nan
            else:
                error_description = np.nan
                error_code = 1
                tariff_profile_type = 'Multi-day variable'
        
        # now we have the day profile index, and we can return it, and subsequently calculate the tariff given the time of day
        return day_profile_index, error_description, error_code, tariff_profile_type
    
    def parse_season_date(self,
                          input_datetime,
                          season_year,
                          season_month,
                          season_day,
                          season_dayofweek):
        day_profile_index = np.nan
        # if year and dayofweek are * then first day of year is index 1 and count from there
        if season_year == '*' and season_dayofweek == '*' and season_month == '1' and season_day == '1':
            # need to determine how many days between start of year and input_datetime
            start_of_year = pd.to_datetime({'year':[input_datetime.year],'month':[1],'day':[1]}, utc=True)
            this_timedelta = input_datetime - start_of_year
            num_days = this_timedelta.dt.days[0]
            day_profile_index = num_days % 7 + 1
        return day_profile_index
    
    def determine_which_season(self,
                               input_datetime,
                               season_matches):
        # to be run when there are more than one seasons, and to determine 
        # which season applies to the input_datetime
        # returns the week_profile_index for the input_datetime
        week_profile_index = np.nan
        # season_matches is a dataframe containing a row for each non-nan season entry
        # first let's check if it's more than the simple case of 2 seasons, if so raise an error
        if len(season_matches) > 2:
            # return np.nan and this will get picked up as error in get_day_profile_index function
            return week_profile_index
        # otherwise proceed with simple case of 2 seasons
        # check it is the simple case where month and day are populated for each
        if (season_matches.season_month == '*').any() or (season_matches.season_day == '*').any():
            # if either is true then we have a harder set of seasons to parse
            # in this case return np.nan and error will get picked up in get_day_profile_index function
            return week_profile_index
        # check for case where season day is specially coded as "L" which means last day of month
        if (season_matches.season_day == 'L').any():
            # for these cases, we need to recode the season day value to correspond to the last day of the month
            logic = (season_matches.season_day == 'L') & (season_matches.season_month == 2)
            if logic.any():
                season_matches.loc[logic,'season_day'] = 28
            logic = (season_matches.season_day == 'L') & (season_matches.season_month.isin([9,4,6,11]))
            if logic.any():
                season_matches.loc[logic,'season_day'] = 30
            logic = (season_matches.season_day == 'L') & (~season_matches.season_month.isin([2,9,4,6,11]))
            if logic.any():
                season_matches.loc[logic,'season_day'] = 31
                
        # otherwise it is simple case where month and day are populated for the 2 seasons
        # if the input_datetime is greater than season 1 start date and less than
        # season 2 start date, then we are in season 1, else trivially we are in season 2
        logic = season_matches.reference == 'TST_S_1'
        
        season_1_start = pd.to_datetime({'year':[input_datetime.year],
                                         'month':[season_matches.loc[logic,'season_month'].to_numpy()[0]],
                                         'day':[season_matches.loc[logic,'season_day'].to_numpy()[0]]},
                                        utc=True)
        logic = season_matches.reference == 'TST_S_2'
        season_2_start = pd.to_datetime({'year':[input_datetime.year],
                                         'month':[season_matches.loc[logic,'season_month'].to_numpy()[0]],
                                         'day':[season_matches.loc[logic,'season_day'].to_numpy()[0]]},
                                        utc=True)
        # two cases: 
        # case 1: season 1 < season 2
        if season_1_start.to_numpy()[0] < season_2_start.to_numpy()[0]:
            if (input_datetime >= season_1_start).to_numpy()[0] and (input_datetime < season_2_start).to_numpy()[0]:
                week_profile_index = 1
            else:
                week_profile_index = 2
        elif season_1_start.to_numpy()[0] > season_2_start.to_numpy()[0]:
            if (input_datetime >= season_2_start).to_numpy()[0] and (input_datetime < season_1_start).to_numpy()[0]:
                week_profile_index = 2
            else:
                week_profile_index = 1
        else:
            # otherwise this is a complex season profile which we are unable to parse at this stage
            # return np.nan and this error will get picked up in get_day_profile_index function
            week_profile_index = np.nan
            
        return week_profile_index
    
    def get_hh_tariff(self,
                      input_datetime,
                      input_day_profile_index,
                      meter_type):
        # given the datetime, and index for the day profile to use, return the tariff for that time
        # for find the row
        # given a datetime, find latest row corresponding to it
        # find any dates that are equal or before the input datetime
        price_per_unit = np.nan
        tariff_description = np.nan
        patp_error_description = np.nan
        patp_error_code = np.nan
        # if day profile index is nan then leave
        if np.isnan(input_day_profile_index):
            return price_per_unit, patp_error_description,patp_error_code, tariff_description
        # ensure day profile index is an integer
        input_day_profile_index = int(input_day_profile_index)
        logic = self.input_data.datetime <= input_datetime
        # if there aren't any values that match then ... 
        if logic.sum() == 0:
            # no corresponding rows, so return nan (and exit)
            patp_error_description ='No data'
            patp_error_code = 0
            tariff_description = 'Unknown (no data)'
            return price_per_unit, patp_error_description,patp_error_code, tariff_description
        # otherwise there is a row, so proceed and get it as this is the most recent date we have a record for
        row = self.input_data.loc[logic,:].tail(n=1)
        key = row.iat[0,0]
        # now get the value corresponding to the index for the specified day profile
        logic = row.key == key
        # check for unusual error case where season not populated for gas meter but patp tariff stored nonetheless in TTP_1
        if (input_day_profile_index == -1) and (meter_type == 'gas'):
            price_per_unit = row.loc[logic,'TTP_1'].to_numpy()[0] 
            tariff_description = 'Single rate'
            patp_error_description  = 'Missing profile data (gas), assumed flat rate'
            patp_error_code = 4
            tariff_description = 'Flat rate'
            return price_per_unit, patp_error_description,patp_error_code, tariff_description
        
        day_profile_value = row.loc[logic,f'TST_DP_{input_day_profile_index}'].to_numpy()[0] 
        # now parse the value, to understand what tariff is applying,and needs to be looked up
        # first there are occasionally instances where this value is empty
       
        if pd.isna(day_profile_value):
            # these cases are for gas meters, in which case the tariff is held in
            # the 'TTP_1' column
            if meter_type == 'gas':
                price_per_unit = row.loc[logic,'TTP_1'].to_numpy()[0]
                patp_error_description  = 'Missing profile data (gas), assumed flat rate'
                patp_error_code = 4
                tariff_description = 'Flat rate'
                return price_per_unit, patp_error_description,patp_error_code, tariff_description
            if meter_type == 'electricity':
                price_per_unit = np.nan
                patp_error_description = 'Missing profile data. Unable to parse'
                patp_error_code = -1
                tariff_description = 'Unknown (missing profile data)'
                return price_per_unit, patp_error_description,patp_error_code, tariff_description
        # next we need to identify if there are multiple tariffs being applied for this day
        # these will be separated by |
        tou_split = day_profile_value.split('|')
        # if there is only one element in this list, then there is only a single
        # entry, and so there is only one tariff in effect, this can be a TOU tariff (a single one)
        if len(tou_split) == 1:
            # or a block tariff, so we need to determine this
            # is it a TOU tariff?
            string_to_search = 'TOU'
            p = re.compile(string_to_search)
            m = p.search(tou_split[0])
            if not m is None:
                # then this is a TOU tariff, with a single price. 
                # we should look up the relevant 
                # column depending o whether gas or electric
                if meter_type == 'gas':
                    # check for rare nan
                    temp = row.loc[logic,'TTP_1'].to_numpy()[0]
                    if np.isnan(temp):
                        price_per_unit = np.nan
                        patp_error_description='Missing patp data. Unable to parse'
                        patp_error_code = -1
                        tariff_description = 'Unknown (missing patp)'
                    else:
                        price_per_unit = int(temp)
                        tariff_description = 'Flat rate'
                        patp_error_code = 1
                    return price_per_unit, patp_error_description,patp_error_code, tariff_description
                elif meter_type == 'electricity':
                    # check for rare nan
                    temp = row.loc[logic,'TPM_1'].to_numpy()[0]
                    if np.isnan(temp):
                        price_per_unit = np.nan
                        patp_error_description='Missing patp data. Unable to parse'
                        patp_error_code = -1
                        tariff_description = 'Unknown (missing patp)'
                    else:
                        price_per_unit = int(temp)
                        tariff_description = 'Flat rate'
                        patp_error_code = 1
                    return price_per_unit, patp_error_description,patp_error_code, tariff_description
            # otherwise it may be a Block tariff
            string_to_search = 'Block'
            p = re.compile(string_to_search)
            m = p.search(tou_split[0])
            if not m is None:
                # then this is a block tariff
                # we are unable to calculate patp without linking to energy 
                # consumption data, so return nan in this instance
                    
                price_per_unit = np.nan
                patp_error_code = -2
                patp_error_description = 'Block tariff. Unable to parse'
                tariff_description = 'Block tariff'
                return price_per_unit, patp_error_description,patp_error_code, tariff_description
        
        # otherwise there is more than one entry in the tou split, so there's more than one
        # tariff to refer to depending on the time of day
        elif len(tou_split) > 1:
            # then we need to parse the entries so that we can determine which 
            # tariff index to refer to given the time of day
            tou_reference = pd.DataFrame(columns=['datetime','tariff_index'])
            for i in tou_split:
                # this element should always be of the format 'datetime'=TOU_X,
                # so just split on '='
                dt, tariff = i.split('=')
                # dt is just HH:MM:SS, so let's add in the YYYY-MM-DD from the 
                # input datetime
                my_dict={}
                my_dict['datetime'] = pd.to_datetime(f'{input_datetime.year}-{input_datetime.month}-{input_datetime.day} {dt}')
                my_dict['tariff_index'] = tariff
                #tou_reference = tou_reference.append(my_dict, ignore_index=True) # <--- deprecated
                tou_reference = pd.concat([tou_reference,
                                          pd.DataFrame(my_dict, index=[0])],
                                         ignore_index=True)
            # check for case where first entry does not start on 00:00:00, in 
            # which case we need to add a new first row, we explicitly states
            # the TOU index that starts at 00:00:00
            start_of_day = pd.to_datetime(f'{input_datetime.year}-{input_datetime.month}-{input_datetime.day} 00:00:00Z')
            if not start_of_day == tou_reference.iloc[0,1]:
                # then we need to add in a another entry
                my_dict={}
                my_dict['datetime'] = start_of_day
                my_dict['tariff_index'] = tou_reference.iloc[-1,-1]
                tou_reference = pd.concat([tou_reference,
                                          pd.DataFrame(my_dict, index=[0])],
                                         ignore_index=True)
                # and reorder the dataframe
                tou_reference.sort_values(by='datetime',inplace=True)
            # now we can find which entry corresponds to the given datetime
            # what is the latest entry corresponding to the input datetime in the tou_reference
            my_tou_index = tou_reference.loc[tou_reference.datetime <= input_datetime,
                                             :].tail(n=1).tariff_index.to_numpy()[0]
            # this will be a string with the last character corresponding to the index
            # of the tariff in effect
            # usually this references to a TOU e.g. 'TOU_2'
            # however occasionally it will refer to a block tariff e.g. 'Block_2'
            # we need to check which
            tou_or_block, index = my_tou_index.split('_')
            if tou_or_block == 'TOU':
                # fetch the corresponding price, depending on whether gas or electric
                if meter_type == 'gas':
                    tariff_description = 'TOUT'
                    value = row.loc[row.key == key,
                                            f'TTP_{my_tou_index[-1]}'].to_numpy()[0]
                    if np.isnan(value):
                        price_per_unit = np.nan
                        patp_error_description = 'No data'
                        patp_error_code = 0
                        return price_per_unit, patp_error_description,patp_error_code, tariff_description
                    else:
                        price_per_unit = int(value)
                        patp_error_code = 1
                    return price_per_unit, patp_error_description,patp_error_code, tariff_description
                elif meter_type =='electricity':
                    tariff_description = 'TOUT'
                    value = row.loc[row.key == key,
                                            f'TPM_{my_tou_index[-1]}'].to_numpy()[0]
                    if np.isnan(value):
                        price_per_unit = np.nan
                        patp_error_description = 'No data'
                        patp_error_code = 0
                        return price_per_unit, patp_error_description,patp_error_code, tariff_description
                    else:
                        price_per_unit = int(value)
                        patp_error_code = 1
                        return price_per_unit, patp_error_description,patp_error_code, tariff_description                
                    
            elif tou_or_block == 'Block':
                # then it's referring to a block tariff 
                # these are more complicated because they require input about 
                # the level cumulative energy consumption compared to the threshold
                # as not calculating this, return nan 
                price_per_unit = np.nan
                patp_error_code=-2
                patp_error_description = 'Block tariff. Unable to parse'
                tariff_description = 'Block'
                return price_per_unit, patp_error_description,patp_error_code, tariff_description
        return price_per_unit, patp_error_description,patp_error_code, tariff_description
    
    def get_patp(self,
                  input_datetime):
        # first get day profile index
        this_day_profile_index, error_description, error_code, \
            tariff_profile_type = self.get_day_profile_index(input_datetime)
        # check if this has produced an error code
        if error_code < 1:
            # if we already have an error, then return values associated with 
            # this erro
            this_patp = np.nan
            tariff_description = 'Unknown (error parsing profile data)'
            return this_patp, error_description, error_code, tariff_profile_type, \
                tariff_description
        # otherwise we don't have an error from the profile parser
        # proceed to determine patp
        this_hh_tariff, error_description, error_code, \
            tariff_description = self.get_hh_tariff(input_datetime, 
                                                    this_day_profile_index, 
                                                    self.meter)
        # check if there is an error in patp parser
        if error_code < 1:
            this_patp = np.nan
            return this_patp, error_description, error_code, tariff_profile_type, \
                tariff_description
        # otherwise we don;t have an error from patp or profile parsers
        # so proceed to parse the patp scale
        this_patps = self.get_patp_scale(input_datetime)
        if np.isnan(this_patps):
            # if there is no patp scale then use ps price scale
            this_ps = self.get_p_scale(input_datetime)
            if np.isnan(this_ps):
                # if no price scale, then record an error
                this_patp = np.nan
                error_description = 'No scale data or scale error'
                error_code = -9
                return this_patp, error_description, error_code, tariff_profile_type, \
                tariff_description
            else: 
                this_patp = self.convert_using_scale(this_hh_tariff,this_ps)
        else:
            # otherwise there is a patp scale to use
            this_patp = self.convert_using_scale(this_hh_tariff, this_patps)
        # Finally check for errors in value of patp
        # small number of zeros, the data is simply not populated
        if this_patp == 0:
            error_code= -10
            error_description = 'Zero value'
        # a small number of may be negative, which is assumed to be incorrect, so change the sign of these
        elif this_patp < 0:
            temp = -this_patp
            this_patp = temp
            error_code = 3
            error_description = 'Negative value (changed sign)'
        # small number of very large values, which we will assume are errors
        elif this_patp > self.too_large:
            error_code = -4
            error_description = f'Value above {self.too_large}'
        # small number where the price scale is probably incorrect producing v
        # small numbers i.e. < 1p/unit but where there is no way of determining 
        # what it should be
        elif this_patp > 0 and this_patp < 0.01:
            error_code = -3
            error_description = f'Value below {self.too_small}'
        return this_patp, error_description, error_code, tariff_profile_type, \
                tariff_description
        
    def all_equal(self,
                  series):
        a = series.to_numpy()
        
        return (a[0] == a).all()
#%% Tariff Parser class
class Tariff_Parser:
    def __init__(self,
                 original_tariff_data:pd.DataFrame,
                 options:dict) -> None:
        self.original_tariff_data = original_tariff_data.copy()
        self.options = options
        # create a datetime variable to hold the date when the tariff data was fetched
        temp = pd.to_datetime(self.original_tariff_data.date, format='%Y%m%d', utc=True).copy()
        if len(temp)==1:
            self.original_tariff_data.loc[:,'datetime'] = temp.to_numpy()[0]
        else:
            self.original_tariff_data.loc[:,'datetime'] = temp
        # create list of unique PUPRN
        self.list_of_PUPRN = original_tariff_data.PUPRN.unique()
        # create a placeholder for a collection of Tariff class objects which can be queried later
        self.collection_of_tariffs = {}
    
    def generate_collection_of_tariffs(self):
        # this creates tariff objects for all PUPRN and meters in the original
        # tariff data
        # Iterate through the PUPRN and meters, and process each's tariff
        grouped = self.original_tariff_data.groupby(by=['PUPRN',
                                                        'scheduleType'])
        counter = 0
        for name, this_tariff_group in grouped:
            # iterate the counter
            counter += 1
            if self.options['display']:
                if counter%100 == 0:
                    print(f'Counter at: {counter}')
            # create a tariff object to process the tariff for this meter
            this_tariff = Tariff(this_tariff_group,
                                              options={'display':self.options['display']})
            # preprocess tariff data
            this_tariff.preprocess_flags()
            this_tariff.preprocess_standing_charge()
            # add to the collection for later retrieval
            self.collection_of_tariffs[f'{name[0]}_{name[1]}'] = this_tariff

    def produce_simplified_tariff(self):
        '''produces a simplified processed tariff data file
        that mirrors the PUPRN and timestamps in the raw data'''
        # create a placeholder for the results
        results_list = []
        # start a counter to display progress
        counter = 0
        # Iterate through the PUPRN and meters, and process each's tariff
        grouped = self.original_tariff_data.groupby(by=['PUPRN',
                                                        'scheduleType'])
        for name, this_tariff_group in grouped:
            # iterate the counter
            counter += 1
            if self.options['display']:
                if counter%100 == 0:
                    print(f'Counter at: {counter}')
            # get the PUPRN for this tariff group
            this_PUPRN = name[0]
            # fetch the tariff object for this meter
            this_tariff = self.collection_of_tariffs[f'{name[0]}_{name[1]}']
            # get datetimes to iterate over
            for this_datetime in this_tariff.input_data.datetime:
                # get standing charge
                this_sc, error_description, error_code \
                    = this_tariff.get_sc(this_datetime)
                # record standing charge
                row_dict = {}
                row_dict['datetime'] = this_datetime
                row_dict['PUPRN'] = this_PUPRN
                row_dict['meter_type'] = this_tariff.meter
                row_dict['variable'] = 'standing charge'
                row_dict['value_GBP'] = this_sc
                row_dict['error_code'] = error_code
                row_dict['error_description'] = error_description
                # record to results
                results_list.append(row_dict)
                # get primary active tariff price
                this_patp, error_description, error_code, tariff_profile_type, \
                tariff_description = this_tariff.get_patp(this_datetime)
                # record patp
                row_dict = {}
                row_dict['datetime'] = this_datetime
                row_dict['PUPRN'] = this_PUPRN
                row_dict['meter_type'] = this_tariff.meter
                row_dict['variable'] = 'unit cost'
                row_dict['value_GBP'] = this_patp
                row_dict['error_code'] = error_code
                row_dict['error_description'] = error_description
                row_dict['tariff_profile_type'] = tariff_profile_type
                row_dict['tariff_description'] = tariff_description
                results_list.append(row_dict)
        # convert the results list into a dataframe
        self.simplified_tariff = pd.DataFrame(results_list)
    
    def query_tariff(self,PUPRN,
                     schedule_type,
                     variable_or_fixed_cost,
                     timestamp:pd.Timestamp)->float:
        # this method is used to get get a tariff element for a given PUPRN
        # and for a particular timestamp
        # Note this has to be run after the generate_collection_of_tariffs
        # method
        # fetch the tariff for this PUPRN and schedule type
        this_tariff = self.collection_of_tariffs[f'{PUPRN}_{schedule_type}']
        # then get the required tariff component
        if variable_or_fixed_cost == 'variable':
            this_value, error_description, error_code, tariff_profile_type, \
                tariff_description = this_tariff.get_patp(timestamp)
        elif variable_or_fixed_cost == 'fixed':
            this_value, error_description, error_code = this_tariff.get_sc(timestamp)
        return this_value, error_description, error_code
            
#%% main
def main():
    # load original tariff data into a dataframe
    os.chdir(original_tariff_path)
    original_tariff = pd.read_csv(original_tariff_file_name)
    # create a Tariff_Parser object to process the original data
    tariff_parser = Tariff_Parser(original_tariff_data=original_tariff,
                                  options=options)
    # iterate through each meter and generate tariff objects which can be used
    # get get tariff data
    tariff_parser.generate_collection_of_tariffs()
    if options['produce_csv']:
        tariff_parser.produce_simplified_tariff()
        os.chdir(results_path)
        tariff_parser.simplified_tariff.to_csv(results_file_name,index=False)
    # revert to project path
    os.chdir(project_path)

if __name__ == '__main__':
    main()


