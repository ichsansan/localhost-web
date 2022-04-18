FOLDER_NAME = '/root/SourceCode/Reset Page/dst'

UNIT_CONFIG = {
    'TAA1': {
        'HOST': '192.168.1.10',
        'DB': 'db_bat_tja1',
        'TAG_ENABLE_COPT':"EWS102/10FDF:SELENABLE_M.BO01",
        'TAG_ENABLE_SOPT': "EWS102/10CH:SELENABLE.BO01"
    },
    'TAA2': {
        'HOST': '192.168.1.11',
        'DB': 'db_bat_tja2',
        'TAG_ENABLE_COPT':"EWS202/20CH:DI_OPC_01.CIN",
        'TAG_ENABLE_SOPT': "EWS202/20CH:SELENABLE.BO01"
    }
}

TAG_MAPPING = {
    'TAA1': {
        '28': 'WatchdogStatus',
        
        '29': 'EWS102/10SOE:DI1I170412.CIN',
        '30': 'EWS102/10DAS0O:AI1O013305.PNT',
        '31': 'EWS102/10DAS0B:PT1B020701.PNT',
        '32': 'EWS102/10FW:CALC1B.RO03',
        
        '34': 'EWS102/10SOE:DI1I170412.CIN',
        '35': 'EWS102/10DAS0O:AI1O013305.PNT',
        '36': 'EWS102/10DAS0B:PT1B020701.PNT',
        '37': 'EWS102/10FW:CALC1B.RO03',
        '38': 'EWS102/10DAS0D:CT1D013306.PNT',
        '39': 'EWS102/10DAS0D:CT1D013307.PNT',
        '40': 'EWS102/10DAS0E:CT1E046304.PNT',
        '41': 'EWS102/10DAS0D:CT1D013306.PNT',
        '42': 'EWS102/10DAS0E:CT1E046303.PNT',
        '43': 'EWS102/10DAS0E:TC1E047103.PNT',
        '44': 'EWS102/10FFLOW:AIN22.PNT',
        '45': 'EWS102/10DAS0E:CT1E046302.PNT',
        '46': 'EWS102/10DAS0E:CT1E046302.PNT',
        '47': 'EWS102/10DAS0D:CT1D012607.PNT',
        '48': 'EWS102/10DAS0D:CT1D012607.PNT',
        '49': 'EWS102/10DAS0B:ZT1B020107.PNT',
        '50': 'EWS102/10DAS0B:ZT1B021704.PNT',
        '51': 'EWS102/10DAS0B:ZT1B020506.PNT'
    },
    'TAA2': {
        '28': 'WatchdogStatus',

        '29': 'EWS202/20SOE:DI2I170412.CIN',
        '30': 'EWS202/20DAS0O:AI2O013305.PNT',
        '31': 'EWS202/20DAS0B:PT2B020701.PNT',
        '32': 'EWS202/20FW:CALC1B.RO03',
        
        '34': 'EWS202/20SOE:DI2I170412.CIN',
        '35': 'EWS202/20DAS0O:AI2O013305.PNT',
        '36': 'EWS202/20DAS0B:PT2B020701.PNT',
        '37': 'EWS202/20FW:CALC1B.RO03',
        '38': 'EWS202/20DAS0D:CT2D013306.PNT',
        '39': 'EWS202/20DAS0D:CT2D013307.PNT',
        '40': 'EWS202/20DAS0E:CT2E046304.PNT',
        '41': 'EWS202/20DAS0D:CT2D013306.PNT',
        '42': 'EWS202/20DAS0E:CT2E046303.PNT',
        '43': 'EWS202/20DAS0E:TC2E047103.PNT',
        '44': 'EWS202/20SVB:FEGT_2.PNT',
        '45': 'EWS202/20DAS0E:CT2E046302.PNT',
        '46': 'EWS202/20DAS0E:CT2E046302.PNT',
        '47': 'EWS202/20DAS0D:CT2D012607.PNT',
        '48': 'EWS202/20DAS0D:CT2D012607.PNT',
        '49': 'EWS202/20DAS0B:ZT2B020107.PNT',
        '50': 'EWS202/20DAS0B:ZT2B021704.PNT',
        '51': 'EWS202/20DAS0B:ZT2B020506.PNT'
    }
}