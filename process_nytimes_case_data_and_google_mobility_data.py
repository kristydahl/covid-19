# -*- coding: utf-8 -*-
"""
Created on Thu Jul  2 17:28:28 2020

@author: KDahl
"""
import pandas
import datetime
from datetime import date
import csv

# csvs of nytimes covid case and google mobility data live in different directories
# off of the 'path' branch. likewise, there's a 'county_level_risk' directory for the results
def id_counties_with_inc_new_cases(county_list, destination_list): # county list supplied by JDB as csv
    # specify path
    path = 'C:/Users/kdahl/OneDrive - Union of Concerned Scientists/GIS_data/coronavirus/coronavirus data/'
    
    #add code here to read JDB's csv and get a list of county fips codes to analyze   
    
    with open(path + 'county_level_risk/test_results.csv','wb') as csvfile: # if converting to python3, change to 'w' and add newline=''
        fieldnames_df = pandas.read_csv(path + 'csv_header.csv')
        fieldnames = list(fieldnames_df.columns)
        csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
        csvwriter.writeheader()
        csvwriter = csv.writer(csvfile, delimiter=',')
        for fips_code in county_list:
            # read in the nytimes cases csv and cast datatypes
            covid_df = pandas.read_csv(path + 'nytimes_counties/us-counties_070220.csv')
            covid_df['date'] = pandas.to_datetime(covid_df['date'])  
            
            # select county by fips code
            county_covid_data = covid_df.loc[covid_df['fips'] == fips_code]
            
            # pull data from past 14 days into a new dataframe
            latest_date = county_covid_data['date'].max()
            seven_days_ago = latest_date - datetime.timedelta(days=7)
            cases_latest_date = county_covid_data[county_covid_data['date']==latest_date]['cases'].iloc[0]
            cases_seven_days_ago = county_covid_data[county_covid_data['date']==seven_days_ago]['cases'].iloc[0]
            past_week_ave_daily_new_cases = (cases_latest_date - cases_seven_days_ago)/7
            print('Last week ave new daily cases = '+ str(past_week_ave_daily_new_cases))        
            
            fourteen_days_ago = latest_date - datetime.timedelta(days=14)
            cases_fourteen_days_ago = county_covid_data[county_covid_data['date']==fourteen_days_ago]['cases'].iloc[0]
            previous_week_ave_daily_new_cases = (cases_seven_days_ago - cases_fourteen_days_ago)/7
            print('Previous week ave new daily cases = '+ str(previous_week_ave_daily_new_cases))
    
            if previous_week_ave_daily_new_cases > past_week_ave_daily_new_cases:
                covid_new_cases_trend = 'decreasing'
            if previous_week_ave_daily_new_cases < past_week_ave_daily_new_cases:
                covid_new_cases_trend = 'increasing'
            if previous_week_ave_daily_new_cases == past_week_ave_daily_new_cases:
                covid_new_cases_trend = 'flat'      
            print('Average daily new cases are ' + covid_new_cases_trend)
    
            # Read in google mobility csv and cast datatypes
            mobility_df = pandas.read_csv(path + 'google_community_mobility_data/Global_Mobility_Report.csv')        
            mobility_df['date'] = pandas.to_datetime(mobility_df['date'])        
            #select county by fips code and analyze
            county_mobility_data = mobility_df.loc[mobility_df['census_fips_code'] == fips_code]  
            latest_date_mob = county_mobility_data['date'].max()
            seven_days_ago = latest_date_mob - datetime.timedelta(days=7)
            fourteen_days_ago = latest_date_mob - datetime.timedelta(days=14)
            
            mobility_results_df = pandas.DataFrame()
            for destination in destination_list:
                destination_short = destination.split('_')[0]
                
                past_week_ave_mobility_change = county_mobility_data.loc[(county_mobility_data['date'] > seven_days_ago) & 
                                                         (county_mobility_data['date'] <= latest_date_mob), destination].mean()
                previous_week_ave_mobility_change = county_mobility_data.loc[(county_mobility_data['date'] > fourteen_days_ago) & 
                                                             (county_mobility_data['date'] <= seven_days_ago), destination].mean()
                
                if abs(previous_week_ave_mobility_change - past_week_ave_mobility_change) <= 2:
                    mobility_trend = 'flat' 
                if past_week_ave_mobility_change - previous_week_ave_mobility_change > 2:
                    mobility_trend = 'increasing'
                if past_week_ave_mobility_change - previous_week_ave_mobility_change < -2:
                    mobility_trend = 'decreasing'
               
                print('mobility around ' + destination_short + ' is ' + mobility_trend)
                mobility_results_df = mobility_results_df.append({'destination_type':destination_short,
                                          'mobility_trend':mobility_trend,
                                          'past_week_ave_mobility':past_week_ave_mobility_change,
                                          'previous_week_ave_mobility':previous_week_ave_mobility_change},
                                          ignore_index=True)
            print(mobility_results_df)
            
            # write data to csv (this is ugly but too late at night to develop more elegant solution)
            csvwriter.writerow([fips_code,
                                covid_new_cases_trend,
                                past_week_ave_daily_new_cases,
                                previous_week_ave_daily_new_cases,
                                latest_date,
                                mobility_results_df[mobility_results_df['destination_type']=='retail']['mobility_trend'].iloc[0],                                
                                mobility_results_df[mobility_results_df['destination_type']=='retail']['past_week_ave_mobility'].iloc[0],
                                mobility_results_df[mobility_results_df['destination_type']=='retail']['previous_week_ave_mobility'].iloc[0],
                                mobility_results_df[mobility_results_df['destination_type']=='parks']['mobility_trend'].iloc[0],                                
                                mobility_results_df[mobility_results_df['destination_type']=='parks']['past_week_ave_mobility'].iloc[0],
                                mobility_results_df[mobility_results_df['destination_type']=='parks']['previous_week_ave_mobility'].iloc[0],
                                mobility_results_df[mobility_results_df['destination_type']=='transit']['mobility_trend'].iloc[0],                                
                                mobility_results_df[mobility_results_df['destination_type']=='transit']['past_week_ave_mobility'].iloc[0],
                                mobility_results_df[mobility_results_df['destination_type']=='transit']['previous_week_ave_mobility'].iloc[0],
                                mobility_results_df[mobility_results_df['destination_type']=='workplaces']['mobility_trend'].iloc[0],                                
                                mobility_results_df[mobility_results_df['destination_type']=='workplaces']['past_week_ave_mobility'].iloc[0],
                                mobility_results_df[mobility_results_df['destination_type']=='workplaces']['previous_week_ave_mobility'].iloc[0],
                                mobility_results_df[mobility_results_df['destination_type']=='residential']['mobility_trend'].iloc[0],                                
                                mobility_results_df[mobility_results_df['destination_type']=='residential']['past_week_ave_mobility'].iloc[0],
                                mobility_results_df[mobility_results_df['destination_type']=='residential']['previous_week_ave_mobility'].iloc[0],
                                latest_date_mob])

id_counties_with_inc_new_cases([17031],
                               ['retail_and_recreation_percent_change_from_baseline',
                               'parks_percent_change_from_baseline',
                               'transit_stations_percent_change_from_baseline',
                               'workplaces_percent_change_from_baseline',
                               'residential_percent_change_from_baseline'])