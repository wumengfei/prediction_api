# coding: utf8
import datetime

GBDT_MODEL_DIR = "model/gbdt/"
HEDONIC_MODEL_DIR = "model/hedonic/"

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
                 "toilet_amount", "cookroom_amount", "build_size", "face_code", "build_end_year", "fitment",
                 "dealdate", "property_fee", "is_sales_tax", "is_sole", "is_school_district", "distance_metor",
                 "total_floor", "floor", "balcony_amount", "frame_structure", "garden_amount", "terrace_amount", "district_id"]  # 模型所需要的特征在这里声明

# 用来控制预测某几个月的房价
DEFAULT_MONTH_SHIFT_LST = [-2, -1, 0, 1, 2, 3]
AVG_PRICE_MONTH_CNT = 12  # 加载月均价近N个月的数据
AVG_PRICE_MONTH_CNT_DAY = 3  # 加载日均价近N个月的数据

# Price Model 配置
NOW_DATE = datetime.datetime.now().strftime("%Y%m%d")
NOW_YEAR = datetime.datetime.now().strftime("%Y")

PRICE_FIX_FLAG = 1  # 是否使用价位修正功能
PRICE_FIX_THRESHOLD = 0.05  # 预估价格偏离拟合均价超过这个阈值会触发规则调价
FIX_COEF = 3 #控制数据修复幅度，此参数越大，修的越多

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

# stablize
stablize_flag = False
stablize_day_cnt = 3  # 根据近三天的行为做平滑
max_diff_rate = 0.05  # 如果当前值和近N天的预测值差别过大，则要平滑
incr_factor = 0.8  # 涨价触发时，原始值的贡献比例
decr_factor = 0.9  # 降价出发时，原始值的贡献比例
es_url = "http://10.10.16.52:9200/house_eval/_search"
es_timeout = 0.1  # 调用es的超时时间(s)

LOG_LEVEL = "WARNING"  # DEBUG,TRACE,NOTICE,WARNING,FATAL

MAX_INCR_RATE = 0.05
MAX_DECR_RATE = 0.02

MODEL_DIM = "district"
gov_rule = True
listprice_wgt = 0.975
