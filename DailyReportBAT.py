# Simpanan functions

def get_daily_report_BAT(unitname, datestart, dateend):
    t0 = time.time()
    print('Date:')
    print(f"{datestart} to {dateend}")
    print(type(datestart), type(dateend))

    try: 
        datestart = pd.to_datetime(datestart).strftime('%Y-%m-%d %X')
        dateend = pd.to_datetime(dateend).strftime('%Y-%m-%d %X')
    except: 
        datestart = (pd.to_datetime(time.ctime()).strftime('%Y-%m-%d %X') - pd.to_timedelta(24, 'h')).strftime('%Y-%m-%d %X')
        dateend = pd.to_datetime(time.ctime()).strftime('%Y-%m-%d %X')

    UNITCODE = unitname
    db_config = config.UNIT_CONFIG[UNITCODE]
    con = f'mysql+mysqlconnector://root:P%40ssw0rd@{db_config["HOST"]}:3306/{db_config["DB"]}'
    
    TAG_ENABLE_COPT = db_config['TAG_ENABLE_COPT']
    TAG_ENABLE_SOPT = db_config['TAG_ENABLE_SOPT']

    # Boiler Auto Tuning Enable Status
    print('Get historical status ...')
    q = f"""SELECT f_address_no, f_date_rec, f_value FROM tb_bat_history 
        WHERE f_address_no IN ("{TAG_ENABLE_COPT}","{TAG_ENABLE_SOPT}")
        AND f_date_rec BETWEEN "{datestart}" AND "{dateend}" """
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
        AND raw.f_date_rec BETWEEN "{datestart}" AND "{dateend}"
        UNION
        SELECT 0 AS f_sequence, f_date_rec, '(' as f_bracket_open, f_address_no , f_address_no AS f_description, f_value , '>0)' AS f_bracket_close 
        FROM tb_bat_history
        WHERE f_address_no = "WatchdogStatus"
        AND f_date_rec BETWEEN "{datestart}" AND "{dateend}" """
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
            AND raw.f_date_rec BETWEEN "{datestart}" AND "{dateend}"
            UNION
            SELECT 0 AS f_sequence, f_date_rec, '(' as f_bracket_open, f_address_no , f_address_no AS f_description, f_value , '>0)' AS f_bracket_close 
            FROM tb_bat_history
            WHERE f_address_no = "WatchdogStatus"
            AND f_date_rec BETWEEN "{datestart}" AND "{dateend}" """
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
        'date': time.strftime('%Y-%m-%d'),
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