import pandas as pd
import numpy as np
import openpyxl as xl
import os, time, re, config

def write_sheet(sheet, cell_number, data):
    if type(data) not in (float, int, bool, str): data = str(data)
    sheet[str(cell_number)] = data
    return sheet

def write_dict(dictname, key, data):
    dictname[key] = data
    return dictname

def generate_sheet(unitname, datestart = 'now', dateend = 'now'):
    datestart = pd.to_datetime(datestart).strftime('%Y-%m-%d %X')
    dateend = pd.to_datetime(dateend).strftime('%Y-%m-%d %X')

    print(f"Generate {unitname} on {datestart} to {dateend}")

    db_config = config.UNIT_CONFIG[unitname]
    con = f'mysql+mysqlconnector://root:P%40ssw0rd@{db_config["HOST"]}/{db_config["DB"]}'

    TAG_ENABLE_COPT = db_config['TAG_ENABLE_COPT']
    TAG_ENABLE_SOPT = db_config['TAG_ENABLE_SOPT']
    starttime, endtime = ['','']

    # Boiler Auto Tuning Enable Status
    print('Get historical status ...')
    q = f"""SELECT f_address_no, f_date_rec, f_value FROM tb_bat_history 
        WHERE f_address_no IN ("{TAG_ENABLE_COPT}","{TAG_ENABLE_SOPT}")
        AND f_date_rec BETWEEN "{datestart}" AND "{dateend}" """

    df = pd.read_sql(q, con)
    df['f_value'] = df['f_value'].astype(float)
    Enable_status = df.pivot('f_date_rec', 'f_address_no', 'f_value')
    Enable_status['BAT'] = Enable_status.max(axis=1)

    Total_minutes = len(Enable_status.resample('1min').mean())
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
        AND raw.f_date_rec BETWEEN "{datestart}" AND "{dateend}"
        AND dtl.f_is_active = 1 """
        # TODO: Waiting for watchdog fixing
        # UNION
        # SELECT 0 AS f_sequence, f_date_rec, '(' as f_bracket_open, f_address_no , f_address_no AS f_description, f_value , '=1)' AS f_bracket_close 
        # FROM tb_bat_history
        # WHERE f_address_no = "WatchdogStatus"
        # AND f_date_rec BETWEEN "{datestart}" AND "{dateend}" """
    df = pd.read_sql(q, con)
    df['f_value'] = df['f_value'].astype(float)
    df['rule'] = df['f_bracket_open'] + df['f_value'].astype(str) + df['f_bracket_close'].str.replace(" AND","")
    df['result'] = [eval(f.replace('=','==')) for f in df['rule']]
    df['equation'] = df['f_bracket_open'] + df['f_description'].astype(str) + df['f_bracket_close'].str.replace(" AND","")
    df['equation'] = df['equation'].str.slice(1,-1)
    
    Safeguard_SOPT = df.pivot_table(index='f_date_rec', columns='equation', values='result')
    Safeguard_SOPT['Safeguard SOPT'] = Safeguard_SOPT.min(axis=1)

    Safeguard_sum_SOPT = pd.DataFrame(Safeguard_SOPT.sum(), columns=['Enabled'])
    Safeguard_sum_SOPT['Disabled'] = Total_minutes - Safeguard_sum_SOPT['Enabled']

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
            AND raw.f_date_rec BETWEEN "{datestart} " AND "{dateend}" 
            AND dtl.f_is_active = 1 """
            # TODO: Waiting for watchdog fixing
            # UNION
            # SELECT 0 AS f_sequence, f_date_rec, '(' as f_bracket_open, f_address_no , f_address_no AS f_description, f_value , '=1)' AS f_bracket_close 
            # FROM tb_bat_history
            # WHERE f_address_no = "WatchdogStatus"
            # AND f_date_rec BETWEEN "{datestart}" AND "{dateend}" """
    df = pd.read_sql(q, con)
    df['f_value'] = df['f_value'].astype(float)
    df['rule'] = df['f_bracket_open'] + df['f_value'].astype(str) + df['f_bracket_close'].str.replace(" AND","")
    df['result'] = [eval(f.replace('=','==')) for f in df['rule']]
    df['equation'] = df['f_bracket_open'] + df['f_description'].astype(str) + df['f_bracket_close'].str.replace(" AND","")
    df['equation'] = df['equation'].str.slice(1,-1)
    
    Safeguard_COPT = df.pivot_table(index='f_date_rec', columns='equation', values='result')
    Safeguard_COPT['Safeguard COPT'] = Safeguard_COPT.min(axis=1)

    Safeguard_sum_COPT = pd.DataFrame(Safeguard_COPT.sum(), columns=['Enabled'])
    Safeguard_sum_COPT['Disabled'] = Total_minutes - Safeguard_sum_COPT['Enabled']

    Safeguard = Safeguard_COPT.merge(Safeguard_SOPT.astype(float), how='left', left_index=True, right_index=True)
    Safeguard_sum = pd.DataFrame(Safeguard.sum(), columns=['Enabled'])
    Safeguard_sum['Disabled'] = Total_minutes - Safeguard_sum['Enabled']

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
    Result = {}

    Result = {
        # Header
        'date': pd.to_datetime('now').strftime('%Y-%m-%d'),
        'AP5': pd.to_datetime('now').strftime('%Y-%m-%d'),
        'F64': f'This document is generated by bot on {time.strftime("%Y-%m-%d %X")}.',
        'I17': f"Unit - {unitname[-1]}",
        'I19' : f"{datestart} to {dateend}",

        # Cells no 1
        'U23': Enable_status_sum['BAT'],
        'AD23': Total_minutes - Enable_status_sum['BAT'],
        'U24': Enable_status_sum[TAG_ENABLE_SOPT],
        'AD24': Total_minutes - Enable_status_sum[TAG_ENABLE_SOPT],
        'U25': Enable_status_sum[TAG_ENABLE_COPT],
        'AD25': Total_minutes - Enable_status_sum[TAG_ENABLE_COPT],

        # Cells no 2
        # TODO: Waiting for watchdog fixing
        # 'U28': Safeguard_sum_SOPT.loc['WatchdogStatus=1', 'Enabled'],
        # 'AD28': Safeguard_sum_SOPT.loc['WatchdogStatus=1', 'Disabled'],
        
        # Cells no 3
        'U54': Possible_df.loc['BAT', 'Enabled'],
        'AD54': Possible_df.loc['BAT', 'Disabled'],
        'U55': Possible_df.loc['Safeguard SOPT', 'Enabled'],
        'AD55': Possible_df.loc['Safeguard SOPT', 'Disabled'],
        'U56': Possible_df.loc['Safeguard COPT', 'Enabled'],
        'AD56': Possible_df.loc['Safeguard COPT', 'Disabled'],

        # Cells no 4
        'U58': round(Enabled_percentage_df.loc['BAT', 'Enabled'], 2),
        'AD58': round(Enabled_percentage_df.loc['BAT', 'Disabled'], 2),
        'U59': round(Enabled_percentage_df.loc['Safeguard SOPT', 'Enabled'], 2),
        'AD59': round(Enabled_percentage_df.loc['Safeguard SOPT', 'Disabled'], 2),
        'U60': round(Enabled_percentage_df.loc['Safeguard COPT', 'Enabled'], 2),
        'AD60': round(Enabled_percentage_df.loc['Safeguard COPT', 'Disabled'], 2),
    }

    # Sootblow safeguard violated
    row = 30
    for idx in Safeguard_sum_SOPT.index:
        if ('Safeguard' in idx) or ('Watchdog' in idx): continue
        Result[f'F{row}'] = idx
        Result[f'U{row}'] = Safeguard_sum_SOPT.loc[idx, 'Enabled']
        Result[f'AD{row}'] = Safeguard_sum_SOPT.loc[idx, 'Disabled']
        row += 1
    
    while row <= 34:
        Result[f'F{row}'] = ''
        Result[f'U{row}'] = ''
        Result[f'AD{row}'] = ''
        row += 1

    # Combustion safeguard violated
    row = 35
    for idx in Safeguard_sum_COPT.index:
        if ('Safeguard' in idx) or ('Watchdog' in idx): continue
        Result[f'F{row}'] = idx
        Result[f'U{row}'] = Safeguard_sum_COPT.loc[idx, 'Enabled']
        Result[f'AD{row}'] = Safeguard_sum_COPT.loc[idx, 'Disabled']
        row += 1
    
    while row <= 52:
        Result[f'F{row}'] = ''
        Result[f'U{row}'] = ''
        Result[f'AD{row}'] = ''
        row += 1

    return Result

def generate_home(s1, s2):
    for k in s1.keys():
        if 'GEN ACTIVE' in str(s1[k]):
            mw_loc = k.replace('F','AD')
    home = {
        'G7': s1['I19'],
        
        'L11': round(s1['U23'] / 60, 2),
        'L13': round((s1['U23'] + s1['AD23']) / 60, 2),
        'L15': round(s1['AD54'] / 60, 2),
        'L16': round(s1['AD54'] / 60, 2),
        'L17': '-',
        'L18': '-',
        'L19': round(s1[mw_loc] / 60, 2),
        'L20': '-',
        'L21': '-',
        'L23': round((s1['AD59'] * s1['U55'] / 100) / 60, 2), # Waktu disable oleh operator
        'L25': round(s1['U54'] / 60, 2),

        'Y11': round(s2['U23'] / 60, 2),
        'Y13': round((s2['U23'] + s2['AD23']) / 60, 2),
        'Y15': round(s2['AD54'] / 60, 2),
        'Y16': round(s2['AD54'] / 60, 2),
        'Y17': '-',
        'Y18': '-',
        'Y19': round(s2[mw_loc] / 60, 2),
        'Y20': '-',
        'Y21': '-',
        'Y23': round((s2['AD59'] * s2['U55'] / 100) / 60, 2), # TODO: Waktu disable oleh operator
        'Y25': round(s2['U54'] / 60, 2),

        'G32': round((s1['U23'] + s2['U23']) / 60, 2),
        'G33': round((s1['U54'] + s2['U54']) / 60, 2),
        'G35': f"{round(100 * (s1['U23'] + s2['U23']) / (s1['U54'] + s2['U54']),2)} %"
    }

    for k in home.keys(): 
        try: home[k] = str(round(home[k]))
        except: pass
    
    return home

def generate_report(datestart = '', dateend = '', kind="Excel"):
    if dateend == '' or dateend == None: 
        dateend = pd.to_datetime('now').strftime('%Y-%m-%d 23:59')
    s1 = generate_sheet('TAA1', datestart, dateend)
    s2 = generate_sheet('TAA2', datestart, dateend)

    s0 = generate_home(s1, s2)

    if kind == "Excel":
        ## Writing to Excel
        print('Writing to excel ...')
        filesrc = 'src/Perhitungan Kertas Kerja BATTJA rev1.xlsx'
        srcfile = xl.load_workbook(filesrc, read_only=False)

        WS0 = srcfile.get_sheet_by_name('Home')
        for k in s0.keys():
            if k not in ['date']:
                write_sheet(WS0, k, s0[k])
            else:
                if k == 'date': write_sheet(WS0, 'AP5', s0[k])

        WS1 = srcfile.get_sheet_by_name('1')
        for k in s1.keys():
            if k not in ['date']:
                write_sheet(WS1, k, s1[k])
            else:
                if k == 'date': write_sheet(WS1, 'AP5', s1[k])
        
        WS2 = srcfile.get_sheet_by_name('2')
        for k in s2.keys():
            if k not in ['date']:
                write_sheet(WS2, k, s2[k])
            else:
                if k == 'date': write_sheet(WS2, 'AP5', s2[k])

        dateprint = datestart
        if datestart != dateend: dateprint = f"{datestart} - {dateend}"
        filename = f"Perhitungan Kertas Kerja BATTJA {dateprint}.xlsx".replace(':','')
        filedst = f"dst/{filename}"
        srcfile.save(filedst)

        return filename
    
    else:
        data = {
            'home': s0,
            's1': s1,
            's2': s2
        }
        return data

if __name__ == '__main__':
    print(generate_report('2022-04-19 12:51', '2022-04-19 20:50'))