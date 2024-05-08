import nfl_data_py as nfl
from pprint import pprint
import connect as ct
import datetime as datetime

def get_data(year_list):
    inj_year = list([(i) for i in year_list if i > 2008])
    pfr_year = list([(i) for i in year_list if i > 2018])

    main_df = nfl.import_weekly_data(year_list, downcast=False)
    pbp_df = nfl.import_pbp_data(year_list, downcast=False)
    season_df = nfl.import_seasonal_data(year_list)
    win_tot_df = nfl.import_win_totals(year_list)
    sc_line_df = nfl.import_sc_lines(year_list)
    schedule_df = nfl.import_schedules(year_list)
    player_ids_df = nfl.import_ids()
    ngs_pass_df = nfl.import_ngs_data('passing', year_list)
    ngs_run_df = nfl.import_ngs_data('rushing', year_list)
    ngs_rec_df = nfl.import_ngs_data('receiving', year_list)
    injury_df = nfl.import_injuries(inj_year)
    pfr_passing_df = nfl.import_seasonal_pfr('pass', pfr_year)
    pfr_rec_df = nfl.import_seasonal_pfr('rec', pfr_year)
    pfr_rush_df = nfl.import_seasonal_pfr('rush', pfr_year)
    
    nfl_data = [main_df, pbp_df, season_df, win_tot_df,
                sc_line_df, schedule_df, player_ids_df, ngs_pass_df,
                ngs_run_df, ngs_rec_df, injury_df, pfr_passing_df, 
                pfr_rec_df, pfr_rush_df]
    
    # Assuming nfl.clean_nfl_data(dataframe) is a function that needs to be defined or imported
    for dataframe in nfl_data:
        nfl.clean_nfl_data(dataframe)
    
    return nfl_data

def push_to_db(dataframes, eng):
    table_names = ['Weekly_Stats', 'PBP_Data', 'Season_Stats', 'Win_Total_Stats',
                   'Score_Line_Stats', 'Schedule_History', 'Player_IDs', 'NGS_Pass_Stats',
                   'NGS_Run_Stats', 'NGS_Rec_Stats', 'Injury_Stats', 'PFR_Pass_Stats',
                   'PFR_Rec_Stats', 'PFR_Run_Stats']
    
    for dataframe, table_name in zip(dataframes, table_names):
        dataframe.to_sql(table_name, eng, if_exists='replace', index=True)

if __name__ == '__main__': 
    current_year = datetime.datetime.now().year
    years = list(range(1999, current_year + 1))
    eng = ct.create_engine_conn()
    dfs = get_data(years)
    push_to_db(dfs, eng)
