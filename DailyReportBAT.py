import numpy as np
import pandas as pd
import os, time, re, config

UNITCODE = 'TAA1'
db_config = config.UNIT_CONFIG[UNITCODE]

t0 = time.time()
con = f'mysql+mysqlconnector://root:P@ssw0rd@{db_config["HOST"]}:3306/{db_config["DB"]}'
date = '2022-03-25'
starttime, endtime = ['00:00','23:59']

TAG_ENABLE_COPT = "EWS102/10FDF:SELENABLE_M.BO01"
TAG_ENABLE_SOPT = "EWS102/10CH:SELENABLE.BO01"


# Boiler Auto Tuning Enable Status
print('Get historical status ...')
q = f"""SELECT f_address_no, f_date_rec, f_value FROM tb_bat_history 
    WHERE f_address_no IN ("{TAG_ENABLE_COPT}","{TAG_ENABLE_SOPT}")
    AND f_date_rec BETWEEN "{date} {starttime}" AND "{date} {endtime}" """
df = pd.read_sql(q, con)
df['f_value'] = df['f_value'].astype(float)
Enable_status = df.pivot('f_date_rec', 'f_address_no', 'f_value')
Enable_status['BAT'] = Enable_status.max(axis=1)
Enable_status_sum = Enable_status.sum()

# SOPT Impossible Condition to Enable
print('Get historical SOPT safeguards ...')
q = f"""SELECT dtl.f_sequence, raw.f_date_rec, dtl.f_bracket_open, dtl.f_tag_sensor, 
    conf.f_description, raw.f_value, dtl.f_bracket_close
    FROM tb_sootblow_rules_hdr head
    LEFT JOIN tb_sootblow_rules_dtl dtl 
    ON head.f_rule_hdr_id = dtl.f_rule_hdr_id
    LEFT JOIN tb_tags_read_conf conf
    ON dtl.f_tag_sensor = conf.f_tag_name 
    LEFT JOIN tb_bat_history raw 
    ON dtl.f_tag_sensor = raw.f_address_no
    WHERE head.f_rule_descr = "Safeguard"
    AND raw.f_date_rec BETWEEN "{date} {starttime}" AND "{date} {endtime}" """
df = pd.read_sql(q, con)
df['f_value'] = df['f_value'].astype(float)
df['rule'] = df['f_bracket_open'] + df['f_value'].astype(str) + df['f_bracket_close'].str.replace(" AND","")
df['result'] = [eval(f.replace('=','==')) for f in df['rule']]

Safeguard_SOPT = df.pivot_table(index='f_date_rec', columns='f_description', values='result')
Safeguard_SOPT['Safeguard SOPT'] = Safeguard_SOPT.min(axis=1)

Safeguard_sum_SOPT = pd.DataFrame(Safeguard_SOPT.sum(), columns=['Enabled'])
Safeguard_sum_SOPT['Disabled'] = 1440 - Safeguard_sum_SOPT['Enabled']

# COPT Impossible Condition to Enable
print('Get historical COPT safeguards ...')
q = f"""SELECT dtl.f_sequence, raw.f_date_rec, dtl.f_bracket_open, dtl.f_tag_sensor, 
    conf.f_description, raw.f_value, dtl.f_bracket_close
    FROM tb_combustion_rules_hdr head
    LEFT JOIN tb_combustion_rules_dtl dtl 
    ON head.f_rule_hdr_id = dtl.f_rule_hdr_id
    LEFT JOIN tb_tags_read_conf conf
    ON dtl.f_tag_sensor = conf.f_tag_name 
    LEFT JOIN tb_bat_history raw 
    ON dtl.f_tag_sensor = raw.f_address_no
    WHERE head.f_rule_descr = "SAFEGUARD"
    AND raw.f_date_rec BETWEEN "{date} {starttime}" AND "{date} {endtime}" """
df = pd.read_sql(q, con)
df['f_value'] = df['f_value'].astype(float)
df['rule'] = df['f_bracket_open'] + df['f_value'].astype(str) + df['f_bracket_close'].str.replace(" AND","")
df['result'] = [eval(f.replace('=','==')) for f in df['rule']]

Safeguard_COPT = df.pivot_table(index='f_date_rec', columns='f_description', values='result')
Safeguard_COPT['Safeguard COPT'] = Safeguard_COPT.min(axis=1)

Safeguard_sum_COPT = pd.DataFrame(Safeguard_COPT.sum(), columns=['Enabled'])
Safeguard_sum_COPT['Disabled'] = 1440 - Safeguard_sum_COPT['Enabled']

Safeguard = Safeguard_COPT.merge(Safeguard_SOPT.astype(float), how='left', left_index=True, right_index=True)
Safeguard_sum = pd.DataFrame(Safeguard.sum(), columns=['Enabled'])
Safeguard_sum['Disabled'] = 1440 - Safeguard_sum['Enabled']

# Total Possible Condition Time
print('Calculating possible condition to enabled time')
Possible_df = Safeguard_sum.loc[['Safeguard SOPT', 'Safeguard COPT']]
BAT_possible_df = pd.DataFrame(Possible_df.iloc[np.argmax(Possible_df['Enabled'])]).T
BAT_possible_df.index = ['BAT']

Possible_df = Possible_df.append(BAT_possible_df)
Possible_time_SOPT = Possible_df.loc['Safeguard SOPT','Enabled']
Possible_time_COPT = Possible_df.loc['Safeguard COPT','Enabled']

# Percentage of BAT Enabled
Enabled_status_df = pd.DataFrame(Enable_status_sum, columns=['Enabled'])
Enabled_status_df = Enabled_status_df.rename(index={TAG_ENABLE_SOPT:'Safeguard SOPT', TAG_ENABLE_COPT:'Safeguard COPT'})
Enabled_status_df = Enabled_status_df.merge(Possible_df[['Enabled']].rename(columns={'Enabled':'Total'}), how='left', left_index=True, right_index=True)
Enabled_status_df['Disabled'] = Enabled_status_df['Total'] - Enabled_status_df['Enabled']
Enabled_status_df[['Enabled','Disabled','Total']]

pembagi = Enabled_status_df[['Total','Total']]
pembagi.columns = ['Enabled','Disabled']

Enabled_percentage_df = Enabled_status_df[['Enabled','Disabled']] * 100/ pembagi
t1 = time.time()

print('Time elapsed:', round(t1-t0, 2), 'secs.')

## Writing to Excel
# Reading template
print('Writing to dict ...')
ret = {}

# Input data
# BAT status
ret['AO5'] = date
ret['U23'] = Enable_status_sum['BAT']
ret['U24'] = Enable_status_sum[TAG_ENABLE_SOPT]
ret['U25'] = Enable_status_sum[TAG_ENABLE_COPT]

ret['AD23'] = 1440 - Enable_status_sum['BAT']
ret['AD24'] = 1440 - Enable_status_sum[TAG_ENABLE_SOPT]
ret['AD25'] = 1440 - Enable_status_sum[TAG_ENABLE_COPT]

# Safeguard SOPT
ret['U28'] = Safeguard_sum_SOPT.loc['Safeguard SOPT','Enabled']
ret['AD28'] = Safeguard_sum_SOPT.loc['Safeguard SOPT','Disabled']
ret['U29'] = Safeguard_sum_SOPT.loc['BOILER MFT TURB TRIP(SOE)','Enabled']
ret['AD29'] = Safeguard_sum_SOPT.loc['BOILER MFT TURB TRIP(SOE)','Disabled']
ret['U30'] = Safeguard_sum_SOPT.loc['GEN ACTIVE POWER','Enabled']
ret['AD30'] = Safeguard_sum_SOPT.loc['GEN ACTIVE POWER','Disabled']
ret['U31'] = Safeguard_sum_SOPT.loc['FURNACE PRESS #1','Enabled']
ret['AD31'] = Safeguard_sum_SOPT.loc['FURNACE PRESS #1','Disabled']
ret['U32'] = Safeguard_sum_SOPT.loc['BOILER DRUM LEVEL 3','Enabled']
ret['AD32'] = Safeguard_sum_SOPT.loc['BOILER DRUM LEVEL 3','Disabled']

# Safeguard COPT
ret['U34'] = Safeguard_sum_COPT.loc['BOILER MFT TURB TRIP(SOE)','Enabled']
ret['AD34'] = Safeguard_sum_COPT.loc['BOILER MFT TURB TRIP(SOE)','Disabled']
ret['U35'] = Safeguard_sum_COPT.loc['GEN ACTIVE POWER','Enabled']
ret['AD35'] = Safeguard_sum_COPT.loc['GEN ACTIVE POWER','Disabled']
ret['U36'] = Safeguard_sum_COPT.loc['FURNACE PRESS #1','Enabled']
ret['AD36'] = Safeguard_sum_COPT.loc['FURNACE PRESS #1','Disabled']
ret['U37'] = Safeguard_sum_COPT.loc['BOILER DRUM LEVEL 3','Enabled']
ret['AD37'] = Safeguard_sum_COPT.loc['BOILER DRUM LEVEL 3','Disabled']
ret['U38'] = Safeguard_sum_COPT.loc['No.1 ID FAN MOT CURRENT','Enabled']
ret['AD38'] = Safeguard_sum_COPT.loc['No.1 ID FAN MOT CURRENT','Disabled']
ret['U39'] = Safeguard_sum_COPT.loc['No.1 ID FAN MOT CURRENT','Enabled']
ret['AD39'] = Safeguard_sum_COPT.loc['No.1 ID FAN MOT CURRENT','Disabled']
ret['U40'] = Safeguard_sum_COPT.loc['No.2 ID FAN MOT CURRENT','Enabled']
ret['AD40'] = Safeguard_sum_COPT.loc['No.2 ID FAN MOT CURRENT','Disabled']
ret['U41'] = Safeguard_sum_COPT.loc['No.1 PA FAN MOT CURRENT','Enabled']
ret['AD41'] = Safeguard_sum_COPT.loc['No.1 PA FAN MOT CURRENT','Disabled']
ret['U42'] = Safeguard_sum_COPT.loc['No.2 PA FAN MOT CURRENT','Enabled']
ret['AD42'] = Safeguard_sum_COPT.loc['No.2 PA FAN MOT CURRENT','Disabled']
ret['U43'] = Safeguard_sum_COPT.loc['SH.FIN.OUT HEADER L OUT GAS TEMP','Enabled']
ret['AD43'] = Safeguard_sum_COPT.loc['SH.FIN.OUT HEADER L OUT GAS TEMP','Disabled']
ret['U44'] = Safeguard_sum_COPT.loc['FEGT','Enabled']
ret['AD44'] = Safeguard_sum_COPT.loc['FEGT','Disabled']
ret['U45'] = Safeguard_sum_COPT.loc['No2 FD FAN MOT CURRENT','Enabled']
ret['AD45'] = Safeguard_sum_COPT.loc['No2 FD FAN MOT CURRENT','Disabled']
ret['U46'] = Safeguard_sum_COPT.loc['No2 FD FAN MOT CURRENT','Enabled']
ret['AD46'] = Safeguard_sum_COPT.loc['No2 FD FAN MOT CURRENT','Disabled']
ret['U47'] = Safeguard_sum_COPT.loc['No1 FD FAN MOT CURRENT','Enabled']
ret['AD47'] = Safeguard_sum_COPT.loc['No1 FD FAN MOT CURRENT','Disabled']
ret['U48'] = Safeguard_sum_COPT.loc['No1 FD FAN MOT CURRENT','Enabled']
ret['AD48'] = Safeguard_sum_COPT.loc['No1 FD FAN MOT CURRENT','Disabled']
ret['U49'] = Safeguard_sum_COPT.loc['No.1 ID FAN FIXED BLADE POSI','Enabled']
ret['AD49'] = Safeguard_sum_COPT.loc['No.1 ID FAN FIXED BLADE POSI','Disabled']
ret['U50'] = Safeguard_sum_COPT.loc['No.1 ID FAN FIXED BLADE POSI','Enabled']
ret['AD50'] = Safeguard_sum_COPT.loc['No.1 ID FAN FIXED BLADE POSI','Disabled']
ret['U51'] = Safeguard_sum_COPT.loc['No.1 FD FAN MOVABLE BLADE  POSI','Enabled']
ret['AD51'] = Safeguard_sum_COPT.loc['No.1 FD FAN MOVABLE BLADE  POSI','Disabled']

# Total possible condition time
ret['U53'] = Possible_df.loc['BAT','Enabled']

# Percentage of BAT enabled
ret['U55'] = round(Enabled_percentage_df.loc['BAT','Enabled'],2)
ret['AD55'] = round(Enabled_percentage_df.loc['BAT','Disabled'],2)
ret['U56'] = round(Enabled_percentage_df.loc['Safeguard SOPT','Enabled'],2)
ret['AD56'] = round(Enabled_percentage_df.loc['Safeguard SOPT','Disabled'],2)
ret['U57'] = round(Enabled_percentage_df.loc['Safeguard COPT','Enabled'],2)
ret['AD57'] = round(Enabled_percentage_df.loc['Safeguard COPT','Disabled'],2)

ret['F66'] = f'This document is generated by bot on {time.ctime()}'

print('Done!')
print(ret)
