import pandas as pd

# WKY: 工作日, WKD: 周末

HARD_CODE_EX_GRATIA_GLOBAL_FLOOR_MAP = {
    'CN_ZNG1043': {
        'WKY': 38,
        'WKD': 40
    },
    'CN_SZX1162': {
        'WKY': 30,
        'WKD': 35
    },
    'CN_SZX1169': {
        'WKY': 38,
        'WKD': 38
    },
    'CN_HEY1030': {
        'WKY': 38,
        'WKD': 40
    },
    'CN_SZX1256 ': {
        'WKY': 38,
        'WKD': 38
    },
    'CN_HKU1035': {
        'WKY': 40,
        'WKD': 40
    },
    'CN_GGU1116': {
        'WKY': 38,
        'WKD': 45
    },
    'CN_JIL1076 ': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_JIL1040': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_CCN1071': {
        'WKY': 35,
        'WKD': 35
    },
    'CN_JIL1076': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_JIL1077': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_CCN1177': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_QIA1050': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_CNG1429': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_XAN1378': {
        'WKY': 35,
        'WKD': 35
    },
    'CN_JIM1028': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_JIL1066': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_WUH1049': {
        'WKY': 32,
        'WKD': 32
    },
    'CN_YUN1034': {
        'WKY': 35,
        'WKD': 35
    },
    'CN_SUA1019': {
        'WKY': 35,
        'WKD': 35
    },
    'CN_BAH1026': {
        'WKY': 35,
        'WKD': 35
    },
    'CN_CCN1152': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_CGD1255': {
        'WKY': 30,
        'WKD': 30
    },
    'CN_XNG1094': {
        'WKY': 35,
        'WKD': 35
    },
    'CN_YNU1163': {
        'WKY': 35,
        'WKD': 35
    }
}

HARD_CODE_EX_GRATIA_GLOBAL_HOTELS = set(HARD_CODE_EX_GRATIA_GLOBAL_FLOOR_MAP.keys())


def _compose_df():
    oyo_id_lst = list()
    weekday_weekend_lst = list()
    global_floor_price_lst = list()
    for oyo_id, weekday_weekend_price_map in HARD_CODE_EX_GRATIA_GLOBAL_FLOOR_MAP.items():
        for weekday_weekend, price in weekday_weekend_price_map.items():
            oyo_id_lst.append(oyo_id)
            weekday_weekend_lst.append(weekday_weekend)
            global_floor_price_lst.append(price)
    df = pd.DataFrame(
        {'oyo_id': oyo_id_lst, 'weekday_weekend': weekday_weekend_lst, 'global_floor_price': global_floor_price_lst})
    df.drop_duplicates(['oyo_id', 'weekday_weekend'], keep="last", inplace=True)
    return df


HARD_CODE_EX_GRATIA_GLOBAL_FLOOR_DF = _compose_df()
