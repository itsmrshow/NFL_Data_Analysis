import nfl_data_py as nfl
from pprint import pprint
import connect as ct

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
    ngs_pass_df = nfl.import_ngs_data('passing',year_list)
    ngs_run_df = nfl.import_ngs_data('rushing',year_list)
    ngs_rec_df = nfl.import_ngs_data('receiving',year_list)
    injury_df = nfl.import_injuries(inj_year)
    pfr_passing_df = nfl.import_pfr('pass',pfr_year)
    pfr_rec_df = nfl.import_pfr('rec',pfr_year)
    pfr_rush_df = nfl.import_pfr('rush',pfr_year)
    nfl_data = [main_df, pbp_df, season_df, win_tot_df,
    sc_line_df, schedule_df, player_ids_df,ngs_pass_df,
    ngs_run_df, ngs_rec_df, injury_df, pfr_passing_df, pfr_rec_df, pfr_rush_df]
    for dataframe in nfl_data:
        nfl.clean_nfl_data(dataframe)
    return nfl_data
#df_2021 = df_2021.merge(pbp_2021[["player_id","player_name"]],left_on="passer_player_id", right_on="player_id")

def push_to_db(dataframes,eng):
    dataframes[0].to_sql('Weekly_Stats',  eng, if_exists='replace', index=True)
    dataframes[1].to_sql('PBP_Data',  eng, if_exists='replace', index=True)
    dataframes[2].to_sql('Season_Stats',  eng, if_exists='replace', index=True)
    dataframes[3].to_sql('Win_Total_Stats',  eng, if_exists='replace', index=True)
    dataframes[4].to_sql('Score_Line_Stats',  eng, if_exists='replace', index=True)
    dataframes[5].to_sql('Schedule_History',  eng, if_exists='replace', index=True)
    dataframes[6].to_sql('Player_IDs',  eng, if_exists='replace', index=True)
    dataframes[7].to_sql('NGS_Pass_Stats',  eng, if_exists='replace', index=True)
    dataframes[8].to_sql('NGS_Run_Stats',  eng, if_exists='replace', index=True)
    dataframes[9].to_sql('NGS_Rec_Stats',  eng, if_exists='replace', index=True)
    dataframes[10].to_sql('Ingury_Stats',  eng, if_exists='replace', index=True)
    dataframes[11].to_sql('PFR_Pass_Stats',  eng, if_exists='replace', index=True)
    dataframes[12].to_sql('PFR_Rec_Stats',  eng, if_exists='replace', index=True)
    dataframes[13].to_sql('PFR_Run_Stats',  eng, if_exists='replace', index=True)

if __name__ == '__main__': 
    year = 1999
    years = []
    while year < 2023:
        years.append(year)
        year+=1
    yl = [2021]
    eng = ct.create_engine_conn()
    dfs = get_data(years)
    push_to_db(dfs,eng)
    print('hi')
    dfs[0].to_sql('Weekly_Stats',  eng, if_exists='replace', index=True, index_label=('player_id','week','season'))