# coding: utf8
import datetime

GBDT_MODEL_DIR = "model/gbdt/"
HEDONIC_MODEL_DIR = "model/hedonic/"

#redis配置参数
redis_conn_info = {
    "host": "m11164.ares.redis.ljnode.com",\
    "port": 11164,\
    "db": 1
}

# build_type调价策略
build_type_dic = {
    "102200000001": -0.015,
    "102200000002": 0.01,
    "102200000003": 0.005,
    "102200000004": 0.015
}

# 必备特征，如果没有则无法预测
IMPORTANT_FEATURE = ["uuid", "city_id", "resblock_id", "bizcircle_code", "bedroom_amount",\
                      "parlor_amount", "toilet_amount", "build_size",\
                      "face_code", "build_end_year", "total_floor", \
                      "floor", "request_id", "district_id"]

# 房屋拟合均价文件
RESBLOCK_AVG_PRICE_FNAME = "data/trans_price_all_ext.txt"
RESBLOCK_AVG_LIST_PRICE_FNAME = "data/list_price_all_ext.txt"
RESBLOCK_AVG_PRICE_DAY_FNAME = "data/trans_price_api_ext_day.txt"  # 按天计算出的小区成交均价文件
RESBLOCK_AVG_LIST_PRICE_DAY_FNAME = "data/list_price_api_ext_day.txt"  # 按天计算出的小区成交均价文件

FEATURE_LIST = ["uuid", "city_id", "resblock_id", "bizcircle_code", "bedroom_amount", "parlor_amount",
                 "toilet_amount", "build_size", "face_code", "build_end_year", "fitment",
                 "dealdate", "is_five", "is_sole", "max_school_level", "distance_metor",
                 "total_floor", "floor", "district_id"]  # 模型所需要的特征在这里声明

AVG_PRICE_MONTH_CNT = 12  # 加载月均价近N个月的数据
AVG_PRICE_MONTH_CNT_DAY = 3  # 加载日均价近N个月的数据

# Price Model 配置
NOW_DATE = datetime.datetime.now().strftime("%Y%m%d")
NOW_YEAR = datetime.datetime.now().strftime("%Y")

PRICE_FIX_FLAG = 1  # 是否使用价位修正功能
PRICE_FIX_THRESHOLD = 0.05  # 预估价格偏离拟合均价超过这个阈值会触发规则调价
FIX_COEF = 3 #控制数据修复幅度，此参数越大，修的越多

LIST_PRICE_FIX_FLAG = 0 # 0表示不调价，1表示调价
LIST_PRICE_FIX_THRESHOLD = 0.05 # 预估价格偏离挂牌价格超过这个阈值会触发规则调价
LIST_FIX_COEF = 3
FIX_DAY_RANGE = 7 # 对近N天的挂牌价进行price_fix平滑

PENALTY_FACTOR = 0.8  # 对于价差的回补的惩罚因子, 暂时不用
UPDATE_INTERVAL = 3600  # 模型更新时间间隔(单位:s)
UPDATE_LOSS_RATE = -0.5
TARGET_SHAKE_MODEL_LST = ["hedonic", "gbdt"]
TARGET_FIX_MODEL_LST = ["hedonic", "gbdt"]

# 应急调价策略
ADJUST_PRICE_INFO_FNAME = "data/adjust_price_info.txt"
ADJUST_PRICE_MAX_RATE = 0.25

FITMENT_RULE = {
                    "110000": 800,
                    "320100": 650,
                    "440300": 800
               }

FORCE_REASONABLE = True  # 强制标签表现出特征符合常理的相关性

PRICE_RANGE_INFO_FNAME = "data/price_range_info.txt"
PRICE_MAX_RANGE = 0.15

LOG_LEVEL = "NOTICE"  # DEBUG,TRACE,NOTICE,WARNING,FATAL

MAX_INCR_RATE = 0.05
MAX_DECR_RATE = 0.02

MODEL_DIM = "district"
gov_rule = True
listprice_wgt = 0.975
