import pandas as pd
import numpy as np
import subprocess, config, re, time

def get_docker_status():
    cmd_command = "docker ps -a"
    p = subprocess.Popen(cmd_command, stdout=subprocess.PIPE, shell=True)
    raw_data = str(p.communicate()[0].decode())
    data = raw_data.replace('  ', '\t')
    data = data.replace('\t ', '\t')
    while '\t\t' in data: data = data.replace('\t\t', '\t')

    data = [c.split('\t') for c in data.split('\n')]
    header = data[0]
    body = data[1:]

    results = {}
    results['header'] = ('DOCKER NAME', 'IMAGE','CREATED','STATUS')

    body_result = {}
    for b in body:
        if len(b) == 7:
            container_id, image, command, created, status, ports, names = b
        elif len(b) == 6:
            container_id, image, command, created, status, names = b
        else: continue
        body_result[names] = {
            'IMAGE': image,
            'CREATED': created,
            'STATUS': status
        }
    results['body'] = body_result
    return results

def get_bat_status():
    sopt_enable_desc = "SOOT BLOWER OPERATION ON/OFF (Main Start/Stop)"
    copt_enable_desc = "COMBUSTION ENABLE"

    data = {}
    for unitname in config.UNIT_CONFIG.keys():
        db_config = config.UNIT_CONFIG[unitname]
        con = f'mysql+mysqlconnector://root:P@ssw0rd@{db_config["HOST"]}:3306/{db_config["DB"]}'

        q = f"""SELECT conf.f_description, raw.f_value FROM tb_sootblow_conf_tags conf
                LEFT JOIN tb_sootblow_raw raw 
                ON conf.f_tag_name = raw.f_address_no 
                WHERE conf.f_description = "{sopt_enable_desc}"
                UNION 
                SELECT conf.f_description, raw.f_value FROM tb_tags_read_conf conf 
                LEFT JOIN tb_bat_raw raw
                ON conf.f_tag_name = raw.f_address_no 
                WHERE conf.f_description = "{copt_enable_desc}";
                """
        df = pd.read_sql(q, con).set_index('f_description')
        data[f'sootblow{unitname[-1]}'] = df.loc[sopt_enable_desc, 'f_value']
        data[f'combustion{unitname[-1]}'] = df.loc[copt_enable_desc, 'f_value']
    return data

def get_bat_status_old():
    qs = """SELECT raw.f_value FROM tb_sootblow_conf_tags conf
            LEFT JOIN tb_sootblow_raw raw 
            ON conf.f_tag_name = raw.f_address_no 
            WHERE conf.f_description = \\"SOOT BLOWER OPERATION ON/OFF (Main Start/Stop)\\";"""
    qc = """SELECT raw.f_value FROM tb_tags_read_conf conf 
            LEFT JOIN tb_bat_raw raw
            ON conf.f_tag_name = raw.f_address_no 
            WHERE conf.f_description = \\"COMBUSTION ENABLE\\";"""

    ip_tja1 = "192.168.1.10"
    ip_tja2 = "192.168.1.11"

    commands = []
    results = []
    for unitname in config.UNIT_CONFIG.keys():
        ip = config.UNIT_CONFIG[unitname]['HOST']
        db = config.UNIT_CONFIG[unitname]['DB']
        for q in [qs, qc]:
            q = q.replace('\n           ','')
            command = f"""docker exec -i mariadb mysql -h {ip} -P3306 -uroot -pP@ssw0rd -e "USE {db}; {q}" """
            p = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
            result = str(p.communicate()[0].decode()) + ' '
            result = bool(int(re.findall('[0-9]+', result)[0]))
            
            commands.append(command)
            results.append(result)
            print(command)
    
    data = {
        'sootblow1':results[0],
        'sootblow2':results[2],
        'combustion1':results[1],
        'combustion2':results[3],
    }
    return data

def get_daily_report_BAT(unitname, date):
    t0 = time.time()
    print('Date:')
    print(date)
    print(type(date))

    try: date = pd.to_datetime(date).strftime('%Y-%m-%d')
    except: date = pd.to_datetime('now').strftime('%Y-%m-%d')

    UNITCODE = unitname
    db_config = config.UNIT_CONFIG[UNITCODE]
    con = f'mysql+mysqlconnector://root:P%40ssw0rd@{db_config["HOST"]}:3306/{db_config["DB"]}'
    
    TAG_ENABLE_COPT = db_config['TAG_ENABLE_COPT']
    TAG_ENABLE_SOPT = db_config['TAG_ENABLE_SOPT']
    starttime, endtime = ['00:00','23:59']

    # Boiler Auto Tuning Enable Status
    print('Get historical status ...')
    q = f"""SELECT f_address_no, f_date_rec, f_value FROM tb_bat_history 
        WHERE f_address_no IN ("{TAG_ENABLE_COPT}","{TAG_ENABLE_SOPT}")
        AND f_date_rec BETWEEN "{date} {starttime}" AND "{date} {endtime}" """
    print(q, con)
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
        AND raw.f_date_rec BETWEEN "{date} {starttime}" AND "{date} {endtime}"
        UNION
        SELECT 0 AS f_sequence, f_date_rec, '(' as f_bracket_open, f_address_no , f_address_no AS f_description, f_value , '>0)' AS f_bracket_close 
        FROM tb_bat_history
        WHERE f_address_no = "WatchdogStatus"
        AND f_date_rec BETWEEN "{date} {starttime}" AND "{date} {endtime}" """
    df = pd.read_sql(q, con)
    df['f_value'] = df['f_value'].astype(float)
    df['rule'] = df['f_bracket_open'] + df['f_value'].astype(str) + df['f_bracket_close'].str.replace(" AND","")
    df['result'] = [eval(f.replace('=','==')) for f in df['rule']]

    Safeguard_SOPT = df.pivot_table(index='f_date_rec', columns='f_tag_sensor', values='result')
    Safeguard_SOPT['Safeguard SOPT'] = Safeguard_SOPT.min(axis=1)

    Safeguard_sum_SOPT = pd.DataFrame(Safeguard_SOPT.sum(), columns=['Enabled'])
    Safeguard_sum_SOPT['Disabled'] = 1440 - Safeguard_sum_SOPT['Enabled']

    # COPT Impossible Condition to Enable
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
            AND raw.f_date_rec BETWEEN "{date} {starttime}" AND "{date} {endtime}"
            UNION
            SELECT 0 AS f_sequence, f_date_rec, '(' as f_bracket_open, f_address_no , f_address_no AS f_description, f_value , '>0)' AS f_bracket_close 
            FROM tb_bat_history
            WHERE f_address_no = "WatchdogStatus"
            AND f_date_rec BETWEEN "{date} {starttime}" AND "{date} {endtime}" """
    df = pd.read_sql(q, con)
    df['f_value'] = df['f_value'].astype(float)
    df['rule'] = df['f_bracket_open'] + df['f_value'].astype(str) + df['f_bracket_close'].str.replace(" AND","")
    df['result'] = [eval(f.replace('=','==')) for f in df['rule']]

    Safeguard_COPT = df.pivot_table(index='f_date_rec', columns='f_tag_sensor', values='result')
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
    ret = {
        'date': date,
        'remarks': f'This document is generated by bot on {time.ctime()}',
        'unit': f'Unit - {UNITCODE[-1]}',
        
        # BAT status
        'U23': Enable_status_sum['BAT'],
        'U24': Enable_status_sum[TAG_ENABLE_SOPT],
        'U25': Enable_status_sum[TAG_ENABLE_COPT],

        'AD23': 1440 - Enable_status_sum['BAT'],
        'AD24': 1440 - Enable_status_sum[TAG_ENABLE_SOPT],
        'AD25': 1440 - Enable_status_sum[TAG_ENABLE_COPT],

        # Total Possible Condition Time
        'U60': Possible_df.loc['BAT', 'Enabled'],
        'AD60': Possible_df.loc['BAT', 'Disabled'],
        'U61': Possible_df.loc['Safeguard SOPT', 'Enabled'],
        'AD61': Possible_df.loc['Safeguard SOPT', 'Disabled'],
        'U62': Possible_df.loc['Safeguard COPT', 'Enabled'],
        'AD62': Possible_df.loc['Safeguard COPT', 'Disabled'],

        # Percentage of BAT enabled
        'U55': round(Enabled_percentage_df.loc['BAT','Enabled'],2),
        'AD55': round(Enabled_percentage_df.loc['BAT','Disabled'],2),
        'U56': round(Enabled_percentage_df.loc['Safeguard SOPT','Enabled'],2),
        'AD56': round(Enabled_percentage_df.loc['Safeguard SOPT','Disabled'],2),
        'U57': round(Enabled_percentage_df.loc['Safeguard COPT','Enabled'],2),
        'AD57': round(Enabled_percentage_df.loc['Safeguard COPT','Disabled'],2),
    }

    for i in [str(f) for f in range(28,33)]:
        if i in config.TAG_MAPPING[UNITCODE].keys():
            try:
                ret[f"U{i}"] = Safeguard_sum_SOPT.loc[config.TAG_MAPPING[UNITCODE][i], 'Enabled']
                ret[f"AD{i}"] = Safeguard_sum_SOPT.loc[config.TAG_MAPPING[UNITCODE][i], 'Disabled']
            except Exception as e:
                print(f'Error getting variable {i} on Safeguard_sum_SOPT: {e}')
    
    for i in [str(f) for f in range(33,52)]:
        if i in config.TAG_MAPPING[UNITCODE].keys():
            try:
                ret[f"U{i}"] = Safeguard_sum_COPT.loc[config.TAG_MAPPING[UNITCODE][i], 'Enabled']
                ret[f"AD{i}"] = Safeguard_sum_COPT.loc[config.TAG_MAPPING[UNITCODE][i], 'Disabled']
            except Exception as e:
                print(f'Error getting variable {i} on Safeguard_sum_SOPT: {e}')

    
    # Total possible condition time
    ret['U53'] = Possible_df.loc['BAT','Enabled']

    # Percentage of BAT enabled
    ret['U55'] = round(Enabled_percentage_df.loc['BAT','Enabled'],2)
    ret['AD55'] = round(Enabled_percentage_df.loc['BAT','Disabled'],2)
    ret['U56'] = round(Enabled_percentage_df.loc['Safeguard SOPT','Enabled'],2)
    ret['AD56'] = round(Enabled_percentage_df.loc['Safeguard SOPT','Disabled'],2)
    ret['U57'] = round(Enabled_percentage_df.loc['Safeguard COPT','Enabled'],2)
    ret['AD57'] = round(Enabled_percentage_df.loc['Safeguard COPT','Disabled'],2)

    # ret['remarks'] = f'This document is generated by bot on {time.ctime()}'
    # ret['date'] = date

    return ret

def do_restart_services():
    commands = [
        "docker restart subs",
        "docker restart sokket-bat-opc-read",
        "docker restart watchdog",
        "docker restart write",
        "docker restart opc-write-copt"
    ]
    results = ""
    for com in commands:
        try:
            p = subprocess.Popen(com, stdout=subprocess.PIPE, shell=True)
            results += str(p.communicate()[0].decode()) + '\n'
        except Exception as e:
            results += str(e) + '\n'
    return results

if __name__ == '__main__':
    get_bat_status()