#coding: utf-8
__author__ = 'Murphy'

import tornado.ioloop
import tornado.web
import tornado.httpclient

import datetime
import os
import sys
import time
from dateutil.relativedelta import relativedelta

sys.path.append("conf")
sys.path.append("lib")
import log
import conf as conf
import PriceModel
import random
import warnings
import urllib2
import threading
import copy
import calendar

import pandas as pd
import json
import urllib
import traceback
import pdb
import math
import redis


warnings.filterwarnings("ignore", category=DeprecationWarning)

logfile = 'log/house_price_impr.log'
wflogfile = logfile + '.wf'
log.init(conf.LOG_LEVEL, logfile, wflogfile, auto_rotate=True)


PRICE_FIX_FLAG = conf.PRICE_FIX_FLAG  # 1:fix; 0:no fix
LIST_PRICE_FIX_FLAG = conf.LIST_PRICE_FIX_FLAG # 1:fix using list_price; -: no fix
PRICE_FIX_THRESHOLD = conf.PRICE_FIX_THRESHOLD
LIST_PRICE_FIX_THRESHOLD = conf.LIST_PRICE_FIX_THRESHOLD
PENALTY_FACTOR = conf.PENALTY_FACTOR
UPDATE_INTERVAL = conf.UPDATE_INTERVAL
UPDATE_LOSS_RATE = conf.UPDATE_LOSS_RATE

AVG_PRICE_MONTH_CNT = conf.AVG_PRICE_MONTH_CNT  # 加载月均价近N个月的数据
AVG_PRICE_MONTH_CNT_DAY = conf.AVG_PRICE_MONTH_CNT_DAY  # 加载日均价近N个月的数据
MAX_INCR_RATE = conf.MAX_INCR_RATE
MAX_DECR_RATE = conf.MAX_DECR_RATE

FIX_COEF = conf.FIX_COEF
LIST_FIX_COEF = conf.LIST_FIX_COEF
FIX_DAY_RANGE = conf.FIX_DAY_RANGE - 1  # 对挂牌价做平滑的时间范围

class PreparedData:

    def __init__(self):
        self.max_incr_rate = MAX_INCR_RATE
        self.max_decr_rate = MAX_DECR_RATE
        self.gov_rule = conf.gov_rule
        self.listprice_wgt = conf.listprice_wgt
        self.gbdt_model_dict = self.load_model("GBDT", conf.GBDT_MODEL_DIR)  # {'bizcircle_code': model}
        self.resblock2avg_price = self.load_resblock_avg_transprice()  # {'resblock_id': avg_price}
        self.resblock2avg_listprice, self.resblock2avg_incr_rate = self.load_resblock_avg_listprice()
        self.resblock2avg_price_day = self.load_resblock_avg_transprice_day()  # 按天计算的小区交易均价
        self.resblock2avg_listprice_day, self.resblock2avg_incr_rate_day = self.load_resblock_avg_listprice_day()
        self.adjust_info2rate = self.load_adjust_price_info()
        self.key2price_range = self.load_price_range()

    def load_model(self, model_type, model_file_dir):
        log.notice("[\tmodel_type=%s\t]model load start" % model_type)
        model_dict = {}
        for model_fname in os.listdir(model_file_dir):
            if not model_fname.endswith("model"):
                continue
            model_key = model_fname.split(".")[1]
            price_model = PriceModel.PriceModel(model_type, model_key)
            model_dict[model_key] = price_model
        log.notice("[\tmodel_type=%s\tcount=%d\t]model load finished", model_type, len(model_dict))
        return model_dict

    def load_resblock_avg_transprice(self):
        log.notice("[\tfile=%s\t] load resblock avg begin", conf.RESBLOCK_AVG_PRICE_FNAME)
        resblock_avg_price_file = open(conf.RESBLOCK_AVG_PRICE_FNAME, "r")
        resblock2avg_price = {}
        min_stat_date = (datetime.date.today().replace(day=1) - datetime.timedelta(AVG_PRICE_MONTH_CNT * 31)).replace(day=1)
        min_stat_date_str = min_stat_date.strftime('%Y%m')

        for line in resblock_avg_price_file:
            row = line.strip().split("\t")
            rm_type = row[10]
            resblock_id, avg_price, stat_date = row[7], float(row[12]), row[9]
            if stat_date < min_stat_date_str: continue
            resblock2avg_price.setdefault(resblock_id, {})
            resblock2avg_price[resblock_id].setdefault(stat_date, {})
            resblock2avg_price[resblock_id][stat_date][rm_type] = avg_price

        for resblock_id, avg_price_dict in resblock2avg_price.iteritems():
            stat_date_lst = avg_price_dict.keys()
            stat_date_lst.sort(reverse=True)
            latest_date = stat_date_lst[0]
            resblock2avg_price[resblock_id]["latest_date"] = avg_price_dict[latest_date]
        resblock_avg_price_file.close()
        log.notice("[\tfile=%s\t] load resblock average finished, %s resblocks loaded", conf.RESBLOCK_AVG_PRICE_FNAME, len(resblock2avg_price))
        return resblock2avg_price

    def load_resblock_avg_listprice(self):
        log.notice("[\tfile=%s\t] load resblock avg list price begin", conf.RESBLOCK_AVG_LIST_PRICE_FNAME)
        resblock_avg_listprice_file = open(conf.RESBLOCK_AVG_LIST_PRICE_FNAME, "r")
        resblock2avg_listprice = {}
        resblock2avg_incr_rate = {}

        min_stat_date = (datetime.date.today().replace(day=1) - datetime.timedelta(AVG_PRICE_MONTH_CNT * 31)).replace(day=1)
        min_stat_date_str = min_stat_date.strftime('%Y%m')
        for line in resblock_avg_listprice_file:
            row = line.strip().split("\t")
            rm_type = row[10]
            resblock_id, avg_listprice, stat_date = row[7], float(row[12]) * 10000, row[9]
            avg_listprice = avg_listprice * self.listprice_wgt #调节挂牌价格的权重
            if stat_date < min_stat_date_str:
                continue
            resblock2avg_listprice.setdefault(resblock_id, {})
            resblock2avg_listprice[resblock_id].setdefault(stat_date, {})
            resblock2avg_listprice[resblock_id][stat_date][rm_type]= avg_listprice

        for resblock_id, avg_listprice_dict in resblock2avg_listprice.iteritems():
            stat_date_lst = avg_listprice_dict.keys()
            stat_date_lst.sort(reverse=True)
            avg_increment_rate = self.cal_avg_increment(stat_date_lst, avg_listprice_dict, resblock_id)

            resblock2avg_incr_rate[resblock_id] = avg_increment_rate
            latest_date = stat_date_lst[0]
            resblock2avg_listprice[resblock_id]["latest_date"] = avg_listprice_dict[latest_date]
        resblock_avg_listprice_file.close()
        log.notice("[\tfile=%s\t] load resblock average listprice finished, %s resblocks loaded", conf.RESBLOCK_AVG_LIST_PRICE_FNAME, len(resblock2avg_listprice))
        return resblock2avg_listprice, resblock2avg_incr_rate

    def load_resblock_avg_transprice_day(self):
        log.notice("[\tfile=%s\t] load resblock transprice avg of day begin", conf.RESBLOCK_AVG_PRICE_DAY_FNAME)
        resblock_avg_price_file = open(conf.RESBLOCK_AVG_PRICE_DAY_FNAME, "r")
        resblock2avg_price = {}
        min_stat_date = (datetime.date.today().replace(day=1) - datetime.timedelta(AVG_PRICE_MONTH_CNT_DAY * 31)).replace(day=1)
        min_stat_date_str = min_stat_date.strftime('%Y%m%d')

        for line in resblock_avg_price_file:
            row = line.strip().split("\t")
            rm_type = row[10]
            resblock_id, avg_price, stat_date = row[7], float(row[12]), row[9]
            if stat_date < min_stat_date_str:
                continue
            resblock2avg_price.setdefault(resblock_id, {})
            resblock2avg_price[resblock_id].setdefault(stat_date, {})
            resblock2avg_price[resblock_id][stat_date][rm_type] = avg_price

        for resblock_id, avg_price_dict in resblock2avg_price.iteritems():
            stat_date_lst = avg_price_dict.keys()
            stat_date_lst.sort(reverse=True)
            latest_date = stat_date_lst[0]
            resblock2avg_price[resblock_id]["latest_date"] = avg_price_dict[latest_date]
        resblock_avg_price_file.close()
        log.notice("[\tfile=%s\t] load resblock average  finished, %s resblocks of day loaded",
                   conf.RESBLOCK_AVG_PRICE_DAY_FNAME, len(resblock2avg_price))
        return resblock2avg_price

    def load_resblock_avg_listprice_day(self):
        log.notice("[\tfile=%s\t] load resblock avg list price of day begin", conf.RESBLOCK_AVG_LIST_PRICE_DAY_FNAME)
        resblock_avg_listprice_file = open(conf.RESBLOCK_AVG_LIST_PRICE_DAY_FNAME, "r")
        resblock2avg_listprice = {}
        resblock2avg_incr_rate = {}

        min_stat_date = (datetime.date.today().replace(day=1) - datetime.timedelta(AVG_PRICE_MONTH_CNT_DAY * 31)).replace(day=1)
        min_stat_date_str = min_stat_date.strftime('%Y%m%d')
        for line in resblock_avg_listprice_file:
            row = line.strip().split("\t")
            rm_type = row[10]  # 卧室数量
            resblock_id, avg_listprice, stat_date = row[7], float(row[12]) * 10000, row[9]
            avg_listprice = avg_listprice * self.listprice_wgt #调节挂牌价格的权重
            if stat_date < min_stat_date_str:
                continue
            resblock2avg_listprice.setdefault(resblock_id, {})
            resblock2avg_listprice[resblock_id].setdefault(stat_date, {})
            resblock2avg_listprice[resblock_id][stat_date][rm_type] = avg_listprice

        for resblock_id, avg_listprice_dict in resblock2avg_listprice.iteritems():
            stat_date_lst = avg_listprice_dict.keys()
            stat_date_lst.sort(reverse=True)
            avg_increment_rate = self.cal_avg_increment(stat_date_lst, avg_listprice_dict, resblock_id)

            resblock2avg_incr_rate[resblock_id] = avg_increment_rate
            latest_date = stat_date_lst[0]
            resblock2avg_listprice[resblock_id]["latest_date"] = avg_listprice_dict[latest_date]
        resblock_avg_listprice_file.close()
        log.notice("[\tfile=%s\t] load resblock average listprice finished, %s resblocks of day loaded",
                   conf.RESBLOCK_AVG_LIST_PRICE_DAY_FNAME, len(resblock2avg_listprice))
        return resblock2avg_listprice, resblock2avg_incr_rate

    def cal_avg_increment(self, stat_date_lst, avg_listprice_dict, resblock_id):
        # 计算每个月的增长率，最终计算平均，输入日期需要降序排列
        increment_lst = []
        stat_date_lst = stat_date_lst[:3]  # 只用最近三个月的数据计算增长率
        stat_date_lth = len(stat_date_lst)
        for idx, stat_date in enumerate(stat_date_lst):
            older_stat_date_idx = idx + 1
            if idx + 1 >= stat_date_lth: break
            older_stat_date = stat_date_lst[older_stat_date_idx]
            if ("-1" not in avg_listprice_dict[older_stat_date]) or ("-1" not in avg_listprice_dict[stat_date]):
                log.warning("-1 not in warning: %s, %s, %s" % (resblock_id, older_stat_date, stat_date))
                continue
            listprice = avg_listprice_dict[stat_date]["-1"]
            older_listprice = avg_listprice_dict[older_stat_date]["-1"]
            if older_listprice == 0.0:
                older_listprice += 1
            incr_rate = (listprice - older_listprice) / older_listprice
            increment_lst.append(incr_rate)
        avg_increment_rate = 0.001
        avg_increment_rate = sum(increment_lst) / (len(increment_lst) + 0.001)
        if abs(avg_increment_rate) > self.max_incr_rate and avg_increment_rate >= 0:
            avg_increment_rate = self.max_incr_rate
        elif abs(avg_increment_rate) > self.max_decr_rate and avg_increment_rate < 0:
            avg_increment_rate = -1 * self.max_decr_rate
        if self.gov_rule == True:
            avg_increment_rate = abs(avg_increment_rate) * -1 #响应政府政策，强制下跌
        return avg_increment_rate

    def load_adjust_price_info(self):
        """
        dim\tid\trate
		resblock\t1111027382474\t0.05
		bizcircle\t611100412\t0.02
        """
        log.notice("[\tfile=%s\t] load adjust price info begin", conf.ADJUST_PRICE_INFO_FNAME)
        adjust_info2rate = {}
        if not os.path.exists(conf.ADJUST_PRICE_INFO_FNAME):
            return adjust_info2rate
        adjust_price_info_file = open(conf.ADJUST_PRICE_INFO_FNAME, "r")
        max_adjust_rate = float(conf.ADJUST_PRICE_MAX_RATE)
        for line in adjust_price_info_file:
            content = line.strip().split("\t")
            dim_type = content[0]
            id = content[1]
            rate = float(content[2])
            info_key = "%s#%s" % (dim_type, id)
            if abs(rate) > max_adjust_rate:
                log.warning("adjust beyond limit: %s" % line.strip())
                continue
            adjust_info2rate[info_key] = rate
        log.notice("[\tfile=%s\t] load adjust price info finished, load %s info", conf.ADJUST_PRICE_INFO_FNAME, len(adjust_info2rate))
        return adjust_info2rate

    def load_price_range(self):
        """
        dim#id#decr_rate#incr_rate
        resblock#1111027382474#0.05#0.05
        bizcircle#611100412#0.02#0.03
        """
        log.notice("[\tfile=%s\t] load price range begin", conf.PRICE_RANGE_INFO_FNAME)
        key2price_range = {}
        if not os.path.exists(conf.PRICE_RANGE_INFO_FNAME):
            return key2price_range
        price_range_info_file = open(conf.PRICE_RANGE_INFO_FNAME, "r")
        max_price_range = float(conf.PRICE_MAX_RANGE)
        for line in price_range_info_file:
            content = line.strip().split("#")
            dim_type = content[0]
            id = content[1]
            decr_rate = float(content[2])
            incr_rate = float(content[3])
            info_key = "%s#%s" % (dim_type, id)
            if abs(incr_rate) > max_price_range or abs(decr_rate) > max_price_range:
                log.warning("range beyond limit: %s" % line.strip())
                continue
            if decr_rate > incr_rate:
                log.warning("decr beyond incr: %s" % line.strip())
                continue
            key2price_range[info_key] = (decr_rate, incr_rate)
        # print key2price_range
        log.notice("[\tfile=%s\t] load price range info finished, load %s info", conf.PRICE_RANGE_INFO_FNAME, len(key2price_range))
        return key2price_range

global resblock2avg_price
global gbdt_model_dict
#global hedonic_model_dict
global last_update_time
global resblock2avg_incr_rate
global resblock2avg_listprice
global adjust_info2rate
global key2price_range
global update_flag

update_flag = False
prepared_data = PreparedData()
resblock2avg_price = prepared_data.resblock2avg_price
resblock2avg_listprice = prepared_data.resblock2avg_listprice
resblock2avg_incr_rate = prepared_data.resblock2avg_incr_rate
resblock2avg_price_day = prepared_data.resblock2avg_price_day
resblock2avg_listprice_day = prepared_data.resblock2avg_listprice_day
resblock2avg_incr_rate_day = prepared_data.resblock2avg_incr_rate_day

gbdt_model_dict = prepared_data.gbdt_model_dict
adjust_info2rate = prepared_data.adjust_info2rate
key2price_range = prepared_data.key2price_range
last_update_time = time.time()

def update_data():
    try:
        log.notice("update data begin")
        global prepared_data
        global gbdt_model_dict
        global hedonic_model_dict
        global resblock2avg_price
        global resblock2avg_listprice
        global resblock2avg_incr_rate
        global resblock2avg_price_day
        global resblock2avg_listprice_day
        global resblock2avg_incr_rate_day
        global adjust_info2rate
        global key2price_range

        tmp_adjust_info2rate = prepared_data.load_adjust_price_info()
        tmp_key2price_range = prepared_data.load_price_range()
        adjust_info2rate = tmp_adjust_info2rate
        key2price_range = tmp_key2price_range

        # 模型要求，如果小于一个阈值，则不更新模型
        tmp_gbdt_model_dict = prepared_data.load_model("GBDT", conf.GBDT_MODEL_DIR)
        gbdt_model_dict_change_rate = (len(tmp_gbdt_model_dict) - len(gbdt_model_dict)) / (len(gbdt_model_dict) + 0.001)
        if gbdt_model_dict_change_rate < UPDATE_LOSS_RATE:
            log.fatal("[\tlvl=ERROR\terror=update gbdt_model_dict abandoned, loss rate: %.3f%%]", gbdt_model_dict_change_rate * 100)
        else:
            gbdt_model_dict = tmp_gbdt_model_dict

        # 月均价更新要求，如果小于一个阈值则不更新
        tmp_resblock2avg_price = prepared_data.load_resblock_avg_transprice()
        tmp_resblock2avg_listprice, tmp_resblock2avg_incr_rate = prepared_data.load_resblock_avg_listprice()
        resblock2avg_price_change_rate = (len(tmp_resblock2avg_price) - len(resblock2avg_price)) / (len(resblock2avg_price) + 0.001)
        resblock2avg_listprice_rate = (len(tmp_resblock2avg_listprice) - len(resblock2avg_listprice)) / (len(resblock2avg_listprice) + 0.001)
        resblock2avg_incr_rate = (len(tmp_resblock2avg_incr_rate) - len(resblock2avg_incr_rate)) / (len(resblock2avg_incr_rate) + 0.001)
        if resblock2avg_price_change_rate < UPDATE_LOSS_RATE:
            log.fatal("[\tlvl=ERROR\terror=update resblock2avg_price abandoned, loss rate: %.3f%%]", resblock2avg_price_change_rate * 100)
        else:
            resblock2avg_price = tmp_resblock2avg_price
        if resblock2avg_listprice_rate < UPDATE_LOSS_RATE:
            log.fatal("[\tlvl=ERROR\terror=update resblock2avg_listprice abandoned, loss rate: %.3f%%]", resblock2avg_listprice_rate * 100)
        else:
            resblock2avg_listprice = tmp_resblock2avg_listprice
        if resblock2avg_incr_rate < UPDATE_LOSS_RATE:
            log.fatal("[\tlvl=ERROR\terror=update resblock2avg_incr_rate abandoned, loss rate: %.3f%%]", resblock2avg_incr_rate * 100)
        else:
            resblock2avg_incr_rate = tmp_resblock2avg_incr_rate

        # 日均价更新要求，如果小于一个阈值则不更新
        tmp_resblock2avg_price_day = prepared_data.load_resblock_avg_transprice_day()
        tmp_resblock2avg_listprice_day, tmp_resblock2avg_incr_rate_day = prepared_data.load_resblock_avg_listprice_day()
        resblock2avg_price_change_rate_day = (len(tmp_resblock2avg_price_day) - len(resblock2avg_price_day)) / (len(resblock2avg_price_day) + 0.001)
        resblock2avg_listprice_rate_day = (len(tmp_resblock2avg_listprice_day) - len(resblock2avg_listprice_day)) / (len(resblock2avg_listprice_day) + 0.001)
        resblock2avg_incr_rate_day = (len(tmp_resblock2avg_incr_rate_day) - len(resblock2avg_incr_rate_day)) / (len(resblock2avg_incr_rate_day) + 0.001)
        if resblock2avg_price_change_rate_day < UPDATE_LOSS_RATE:
            log.fatal("[\tlvl=ERROR\terror=update resblock2avg_price abandoned, loss rate: %.3f%%]", resblock2avg_price_change_rate_day * 100)
        else:
            resblock2avg_price_day = tmp_resblock2avg_price_day
        if resblock2avg_listprice_rate_day < UPDATE_LOSS_RATE:
            log.fatal("[\tlvl=ERROR\terror=update resblock2avg_listprice abandoned, loss rate: %.3f%%]", resblock2avg_listprice_rate_day * 100)
        else:
            resblock2avg_listprice_day = tmp_resblock2avg_listprice_day
        if resblock2avg_incr_rate_day < UPDATE_LOSS_RATE:
            log.fatal("[\tlvl=ERROR\terror=update resblock2avg_incr_rate abandoned, loss rate: %.3f%%]", resblock2avg_incr_rate_day * 100)
        else:
            resblock2avg_incr_rate_day = tmp_resblock2avg_incr_rate_day

        test_key = '1111027382474'
        test_trans = resblock2avg_price.get(test_key, {})
        test_list = resblock2avg_listprice.get(test_key, {})
        log.notice("test_trans_up: %s" % (str(test_trans)))
        log.notice("test_list_up: %s" % (str(test_list)))

        log.notice("update data finish")
    except Exception, e:
        traceback.print_exc()
        log.fatal("[\ttraceback=%s\terror=%s\t]", traceback.format_exc(), e)


def set_update_timer():
    global update_flag
    print("initialization update_flag: " + str(update_flag))
    if update_flag == True:
        update_data()
    t = threading.Timer(UPDATE_INTERVAL, set_update_timer)
    update_flag = True
    print("after update_flag: " + str(update_flag))
    t.start()

set_update_timer()


class MainHandler(tornado.web.RequestHandler):

    def initialize(self):
        """
        初始化模型和日均价数据
        """
        self.gbdt_model_dict = gbdt_model_dict
        self.resblock2avg_price_day = resblock2avg_price_day
        self.resblock2avg_listprice_day = resblock2avg_listprice_day
        self.resblock2avg_incr_rate_day = resblock2avg_incr_rate_day
        self.important_ftr_lst = conf.IMPORTANT_FEATURE
        self.feature_lst = conf.FEATURE_LIST
        self.target_shake_model_lst = conf.TARGET_SHAKE_MODEL_LST
        self.target_fix_model_lst = conf.TARGET_FIX_MODEL_LST
        self.fitment_rule = conf.FITMENT_RULE
        self.force_reasonable = conf.FORCE_REASONABLE
        self.model_dim = conf.MODEL_DIM
        self.build_type_dic = conf.build_type_dic

        self.resblock2avg_price = resblock2avg_price
        self.resblock2avg_listprice = resblock2avg_listprice
        self.resblock2avg_incr_rate = resblock2avg_incr_rate

    def validate_feature_val(self, feature, feature_value):
        """
        检查指定特征的值域是否符合要求
        """
        if feature in ["bedroom_amount", "build_size", "total_floor"]:
            if eval(feature_value) <= 0:
                return False
            else:
                return True
        if feature in ["parlor_amount", "toilet_amount", "cookroom_amount", \
                       "balcony_amount", "garden_amount", "terrace_amount"]:
            if not feature_value.isdigit():
                return False
            if eval(feature_value) < 0:
                return False
            else:
                return True
        if feature in ["fitment", "is_five", "is_sole", "max_school_level"]:
            if not feature_value.isdigit():
                return False
            # 2.0版本学区房写法
            if feature == "max_school_level":
                if feature_value not in ["103000000001","103000000002","103000000003","103000000004","103000000005", "0", "1"]:
                    return False
            elif eval(feature_value) not in [0, 1]:
                return False
            else:
                return True
        return True

    def check_request_format(self, rqst_data):
        """
        检查必须的特征是否传过来，检查指定特征的值域是否符合要求
        """
        #pdb.set_trace()
        check_info_lst = []
        for rqst_each in rqst_data:
            check_info = {}
            missed_ftr = []
            bad_val_ftr = []

            check_info["is_ok"] = False
            if len(rqst_data) == 0:
                check_info["error"] = "parameter data is empty"
                check_info_lst.append(check_info)
                continue

            for feature in self.important_ftr_lst:
                if feature not in rqst_each:
                    missed_ftr.append(feature)
            if len(missed_ftr) > 0:
                check_info["error"] = "miss feature: %s" % ",".join(missed_ftr)
                check_info_lst.append(check_info)
                continue

            for ftr_key, ftr_val in rqst_each.iteritems():
                is_ok = self.validate_feature_val(ftr_key, ftr_val)
                if not is_ok:
                    bad_val_ftr.append(ftr_key)
            if len(bad_val_ftr) > 0:
                check_info["error"] = "bad feature value: %s" % ",".join(bad_val_ftr)
                check_info_lst.append(check_info)
                continue

            check_info["is_ok"] = True
            check_info_lst.append(check_info)
        return check_info_lst

    def replenish_lost_feature(self, checked_rqst_data, time_type):
        """
        判断是否有缺失的特征，如果有按默认值补充,并补充额外的日均价特征
        """
        feature_dict = {}
        for feature_name in self.feature_lst:
            # 针对学区/地铁做特殊处理
            if feature_name == "distance_metor":
                feature_dict[feature_name] = str(checked_rqst_data.get(feature_name, '5000'))
            elif feature_name == "max_school_level":
                school_level = checked_rqst_data.get(feature_name, '0')
                if school_level in ["103000000001","103000000002","103000000003","103000000004","103000000005", "1"]:
                    feature_dict[feature_name] = '1'
                else:
                    feature_dict[feature_name] = '0'
            elif feature_name == "is_five":
                feature_dict[feature_name] = str(checked_rqst_data.get(feature_name,"1"))
            else:
                feature_dict[feature_name] = str(checked_rqst_data.get(feature_name, "0"))

        # 补充额外的均价特征,分日和月
        if time_type == "month":
            feature_dict["resblock_trans_price"] = self.resblock2avg_price.get(feature_dict["resblock_id"], {})  # 补充拟合均价特征
            feature_dict["resblock_list_price"] = self.resblock2avg_listprice.get(feature_dict["resblock_id"], {})
            feature_dict["resblock_avg_price_incr_rate"] = self.resblock2avg_incr_rate.get(feature_dict["resblock_id"], 0.0)
        else:
            feature_dict["resblock_trans_price"] = self.resblock2avg_price_day.get(feature_dict["resblock_id"], {})  # 补充拟合均价特征
            feature_dict["resblock_list_price"] = self.resblock2avg_listprice_day.get(feature_dict["resblock_id"], {})
            feature_dict["resblock_avg_price_incr_rate"] = self.resblock2avg_incr_rate_day.get(feature_dict["resblock_id"], 0.0)
        feature_dict["pre_date"] = time.strftime('%Y%m%d', time.localtime())
        return feature_dict

    def generate_target_date(self):
        """
        以当前时间为基准，分别计算前一年后一年时间
        返回每一个月月初
        """
        idx2target_date = {}
        cur_date = time.strftime('%Y%m01', time.localtime(time.time()))
        idx2target_date[0] = cur_date
        for i in range(1, 13):
            lst_idx = -1 * i + 1
            lst_date = idx2target_date[lst_idx]
            lst_date_time = time.strptime(lst_date, '%Y%m%d')
            tmp_date = time.strftime('%Y%m01', time.localtime(time.mktime(lst_date_time) - 24 * 3600))
            idx2target_date[-1 * i] = tmp_date
        for i in range(1, 13):
            lst_idx = i - 1
            lst_date = idx2target_date[lst_idx]
            lst_date_time = time.strptime(lst_date,'%Y%m%d')
            lst_month_day_cnt = calendar.monthrange(lst_date_time[0],lst_date_time[1])[1]
            tmp_date = time.strftime('%Y%m01', time.localtime(time.mktime(lst_date_time) + 24 * 3600 * lst_month_day_cnt))
            idx2target_date[i] = tmp_date
        return idx2target_date

    def get_month_lst(self, start, end):
        # 根据start和end得出的日期，计算间隔的月份,返回对应月份
        start_tmp = datetime.datetime.strptime(start, "%Y%m%d").replace(day=1).strftime("%Y%m%d")  # 转化到月初
        end_tmp = datetime.datetime.strptime(end, "%Y%m%d").replace(day=1).strftime("%Y%m%d")  # 转化到月初
        target_date_lst = []
        idx2target_date = self.generate_target_date()
        for key, date in idx2target_date.iteritems():
            if (date >= start_tmp) and (date <= end_tmp):
                target_date_lst.append(date[:6])
        return target_date_lst

    def do_prediction(self, feature_dict, input_target_time):
        """
        预测逻辑：GBDT能预测就都预测,较上一版去除了hedonic预测
        返回逻辑：返回GBDT的结果
        """

        bizcircle_code = feature_dict.get("bizcircle_code", "")
        resblock_id = feature_dict.get("resblock_id", "")
        district_id = feature_dict.get("district_id", "")

        gbdt_predict_rlt = 0.0
        predict_rlt = {}
        predict_rlt.setdefault("gbdt", [])

        input_target_day = []
        time_type = ""

        # 对输入时间粒度进行判断,6位为月粒度,8位为日粒度,统一输入至input_target_day中

        if len(input_target_time[0]) == 6:
            time_type = "month"
            for target_month in input_target_time:
                target_date = "%s01" % target_month
                input_target_day.append(target_date)
        else:
            time_type = "day"
            for target_day in input_target_time:
                input_target_day.append(target_day)


        #暂没有对resblock粒度的估价需求
        model_key = bizcircle_code
        if self.model_dim == "district":
            model_key = district_id

        if model_key in self.gbdt_model_dict:
            target_gbdt_model = self.gbdt_model_dict[model_key]
            for target_date in input_target_day:
                feature_dict["dealdate"] = target_date   # 20161207格式
                if time_type == "month":
                    target_date = target_date[:6]
                if ("latest_date" not in feature_dict["resblock_trans_price"].keys()) or ("latest_date" not in feature_dict["resblock_list_price"].keys()):
                    break
                resblock2trans_price_default = feature_dict["resblock_trans_price"]["latest_date"]  # 以能取到的最新一天的数据作为默认
                resblock2list_price_default = feature_dict["resblock_list_price"]["latest_date"]  # 以能取到的最新一天的数据作为默认
                resblock2trans_price_info = feature_dict["resblock_trans_price"].get(target_date,
                                                                                     resblock2trans_price_default)
                resblock2list_price_info = feature_dict["resblock_list_price"].get(target_date,
                                                                                   resblock2list_price_default)

                bed_rm_cnt = feature_dict["bedroom_amount"]
                build_size = float(feature_dict["build_size"])

                # 分居室交易均价
                resblock_trans_price_comm = resblock2trans_price_info["-1"]  # 不区分居室的均价
                if bed_rm_cnt > "3":
                    resblock_trans_price_room = resblock2trans_price_info.get("-2", resblock_trans_price_comm)  # 三居室及以上
                else:
                    resblock_trans_price_room = resblock2trans_price_info.get(bed_rm_cnt, resblock_trans_price_comm)  # 区分居室的均价,如没有用comm补

                # 分居室挂牌均价
                resblock_list_price_comm = resblock2list_price_info["-1"]
                if bed_rm_cnt > "3":
                    resblock_list_price_room = resblock2list_price_info.get("-2", resblock_list_price_comm)  # 三居室及以上
                else:
                    resblock_list_price_room = resblock2list_price_info.get(bed_rm_cnt, resblock_list_price_comm)

                resblock_trans_list_avg_room = (resblock_trans_price_room + resblock_list_price_room) / 2.0
                trans_total_price_comm = resblock_trans_price_comm * build_size
                list_total_price_comm = resblock_list_price_comm * build_size
                trans_total_price_room = resblock_trans_price_room * build_size
                list_total_price_room = resblock_list_price_room * build_size
                trans_list_total_price_room = resblock_trans_list_avg_room * build_size

                feature_dict["resblock_trans_price_comm"] = resblock_trans_price_comm
                feature_dict["resblock_trans_price_room"] = resblock_trans_price_room
                feature_dict["resblock_list_price_comm"] = resblock_list_price_comm
                feature_dict["resblock_list_price_room"] = resblock_list_price_room
                feature_dict["resblock_trans_list_avg_room"] = resblock_trans_list_avg_room
                feature_dict["trans_total_price_comm"] = trans_total_price_comm
                feature_dict["list_total_price_comm"] = list_total_price_comm
                feature_dict["trans_total_price_room"] = trans_total_price_room
                feature_dict["list_total_price_room"] = list_total_price_room
                feature_dict["trans_list_total_price_room"] = trans_list_total_price_room

                if self.force_reasonable:  # 强制标签表现出特征符合常理的相关性
                    tmp_feature_dict = copy.copy(feature_dict)
                    tmp_feature_dict["is_five"] = '0'
                    tmp_feature_dict["max_school_level"] = '0'
                    tmp_feature_dict["is_sole"] = '0'
                    tmp_predict_rlt = target_gbdt_model.predict(tmp_feature_dict)
                    if feature_dict["is_sole"] == '1':
                        tmp_predict_rlt *= 1.001
                    if feature_dict["is_five"] == '1':
                        tmp_predict_rlt *= 1.003
                    if feature_dict["max_school_level"] == '1':
                        tmp_predict_rlt *= 1.006
                    gbdt_predict_rlt = tmp_predict_rlt
                else:
                    gbdt_predict_rlt = target_gbdt_model.predict(feature_dict)
                predict_rlt["gbdt"].append((target_date, gbdt_predict_rlt))

        if len(predict_rlt.get("gbdt", [])) != 0:
            predict_rlt["rescode"] = 1
        else:
            predict_rlt["rescode"] = -1
        return predict_rlt

    def fix_large_size(self, predict_price, feature_dict):
        resblock_id = feature_dict["resblock_id"]
        build_size = float(feature_dict["build_size"])
        bed_rm_cnt = feature_dict["bedroom_amount"]
        if resblock_id == "1111027375049" and build_size > 125 and bed_rm_cnt == "3":
            floor = feature_dict["floor"]
            if floor != '1':
                predict_price = predict_price * 0.93
            else:
                predict_price = predict_price * 0.89
        return predict_price

    def adjust_price(self, predict_rlt, feature_dict):
        """
        人工强行对某一商圈，小区进行调价，以应对紧急情况,
        优先调小区粒度的，再优先调商圈粒度的，不可叠加
        """
        bizcircle_code = feature_dict.get("bizcircle_code", "")
        resblock_id = feature_dict.get("resblock_id", "")
        bizcircle_key = "bizcircle#%s" % bizcircle_code
        resblock_key = "resblock#%s" % resblock_id

        if resblock_key in adjust_info2rate:
            rate = adjust_info2rate[resblock_key]
            predict_rlt = predict_rlt * (1 + rate)
        elif bizcircle_key in adjust_info2rate:
            rate = adjust_info2rate[bizcircle_key]
            predict_rlt = predict_rlt * (1 + rate)
        return predict_rlt

    def list_price_fix(self, predict_rlt, feature_dict, request_id, cur_date):
        #对估价原始值过低的预测进行基于小区均价的规则修正
        target_fix_model = self.target_fix_model_lst
        build_size = float(feature_dict["build_size"])
        bed_rm_cnt = feature_dict["bedroom_amount"]
        # 对日和月估价进行对挂牌价的平滑措施
        if len(cur_date) == 6:
            target_fix_date = [cur_date]
        else:
            now = datetime.datetime.strptime(cur_date,"%Y%m%d")
            past = now - datetime.timedelta(days=FIX_DAY_RANGE) # 对挂牌价做平滑的时间范围
            ori_fix_date = pd.period_range(past, now, freq="d")
            target_fix_date = [i.strftime("%Y%m%d") for i in ori_fix_date]

        for model_key in target_fix_model:
            target_predict_rlt = predict_rlt.get(model_key, [])
            fixed_predict_rlt = []
            for idx, each_predict_rlt in enumerate(target_predict_rlt):
                predict_date = each_predict_rlt[0]
                predict_price = each_predict_rlt[1]
                if predict_date not in target_fix_date:
                    fixed_predict_rlt.append((predict_date, predict_price))
                    continue

                if predict_date in feature_dict["resblock_list_price"]:
                    resblock_avg_price_comm = feature_dict["resblock_list_price"][predict_date]["-1"]
                    resblock_avg_price = feature_dict["resblock_list_price"][predict_date].get(bed_rm_cnt, resblock_avg_price_comm)
                else: #default
                    resblock_avg_price_comm = feature_dict["resblock_list_price"]["latest_date"]["-1"]
                    resblock_avg_price = feature_dict["resblock_list_price"]["latest_date"].get(bed_rm_cnt, resblock_avg_price_comm)

                avg_total_price = build_size * resblock_avg_price
                price_diff = predict_price - avg_total_price
                diff_rate = abs(price_diff) / avg_total_price
                acc_err_rate = diff_rate * LIST_FIX_COEF
                ori_predict_price = float(predict_price)

                if acc_err_rate >= 1:
                    acc_err_rate = 1
                if acc_err_rate <= LIST_PRICE_FIX_THRESHOLD * LIST_FIX_COEF:
                    predict_price = ori_predict_price
                else:
                    predict_price = acc_err_rate * avg_total_price + ori_predict_price * (1 - acc_err_rate)

                log.debug("list_price_fix\t%s\t%s\t%s" % (request_id, ori_predict_price, float(predict_price)))
                fixed_predict_rlt.append((predict_date, predict_price))
            predict_rlt[model_key] = fixed_predict_rlt
        return predict_rlt

    def price_fix(self, predict_rlt, feature_dict, request_id, cur_date):
        #对估价原始值过低的预测进行基于小区均价的规则修正
        target_fix_model = self.target_fix_model_lst
        build_size = float(feature_dict["build_size"])
        bed_rm_cnt = feature_dict["bedroom_amount"]
        for model_key in target_fix_model:
            target_predict_rlt = predict_rlt.get(model_key, [])
            fixed_predict_rlt = []
            for idx, each_predict_rlt in enumerate(target_predict_rlt):
                predict_date = each_predict_rlt[0]
                predict_price = each_predict_rlt[1]
                if predict_date in feature_dict["resblock_trans_price"]:
                    resblock_avg_price_comm = feature_dict["resblock_trans_price"][predict_date]["-1"]
                    resblock_avg_price = feature_dict["resblock_trans_price"][predict_date].get(bed_rm_cnt, resblock_avg_price_comm)
                else: #default
                    resblock_avg_price_comm = feature_dict["resblock_trans_price"]["latest_date"]["-1"]
                    resblock_avg_price = feature_dict["resblock_trans_price"]["latest_date"].get(bed_rm_cnt, resblock_avg_price_comm)

                avg_total_price = build_size * resblock_avg_price
                price_diff = predict_price - avg_total_price
                diff_rate = abs(price_diff) / avg_total_price
                acc_err_rate = diff_rate * FIX_COEF
                ori_predict_price = float(predict_price)

                if acc_err_rate >= 1:
                    acc_err_rate = 1
                if acc_err_rate <= PRICE_FIX_THRESHOLD * FIX_COEF:
                    predict_price = ori_predict_price
                else:
                    predict_price = acc_err_rate * avg_total_price + ori_predict_price * (1 - acc_err_rate)

                predict_price = self.adjust_price(predict_price, feature_dict)
                predict_price = self.fix_large_size(predict_price, feature_dict)
                log.debug("price_fix\t%s\t%s\t%s" % (request_id, ori_predict_price, float(predict_price)))
                fixed_predict_rlt.append((predict_date, predict_price))
            predict_rlt[model_key] = fixed_predict_rlt
        return predict_rlt

    def shake_price(self, predict_rlt, feature_dict, request_id, cur_date):
        #根据挂牌价近期走势，对估价结果在时间维度上微调
        target_shake_model = self.target_shake_model_lst
        for model_key in target_shake_model:
            target_predict_rlt = predict_rlt.get(model_key, [])
            shaked_predict_rlt = []
            for idx, each_rlt in enumerate(target_predict_rlt):
                predict_month = each_rlt[0]
                predict_price = each_rlt[1]
                last_idx = idx - 1
                # 如果估价日期在请求日期之前，则不做shake操作
                if idx == 0 or predict_month <= cur_date:
                    shaked_predict_rlt.append(each_rlt)
                    continue
                last_rlt = target_predict_rlt[last_idx]
                last_predict_month = last_rlt[0]
                last_predict_price = last_rlt[1]

                if last_predict_price - predict_price != 0:
                    shaked_predict_rlt.append(each_rlt)
                else:
                    time_seed = int(time.strftime("%Y%m%d" ,time.localtime()))
                    random.seed(time_seed) #保证同一天的随机因子是一致的
                    random_vibrate = round((random.random() + 0.01) /1000, 5) #加入随机抖动值,让价格波动微微变化
                    resblock_avg_price_incr_rate = feature_dict["resblock_avg_price_incr_rate"]
                    if resblock_avg_price_incr_rate > 0.01:
                        new_predict_price = shaked_predict_rlt[last_idx][1] * (1 + resblock_avg_price_incr_rate / idx + random_vibrate)
                    else:
                        new_predict_price = shaked_predict_rlt[last_idx][1]* (1 + resblock_avg_price_incr_rate + random_vibrate)
                    shaked_predict_rlt.append((predict_month, new_predict_price))
                    log.debug("price_shake\t%s\t%s\t%s" % (request_id, float(predict_price), float(new_predict_price)))
            predict_rlt[model_key] = shaked_predict_rlt
        return predict_rlt

    def get_price_range(self, predict_rlt_details, feature_dict):

        bizcircle_code = feature_dict.get("bizcircle_code", "")
        resblock_id = feature_dict.get("resblock_id", "")
        bizcircle_key = "bizcircle#%s" % bizcircle_code
        resblock_key = "resblock#%s" % resblock_id
        range_info = (-0.05, 0.05)
        range_rlt_lst = []
        if resblock_key in key2price_range:
            range_info = key2price_range[resblock_key]
        elif bizcircle_key in key2price_range:
            range_info = key2price_range[bizcircle_key]
        for predict_rlt in predict_rlt_details.split("#"):
            min_rlt = float(math.fabs(range_info[0]))
            max_rlt = float(math.fabs(range_info[1]))
            range_rlt = "%.2f,%.2f" % (min_rlt, max_rlt)
            range_rlt_lst.append(range_rlt)
        range_rlt_str = "#".join(range_rlt_lst)
        return range_rlt_str

    def apply_rule(self, predict_rlt_details, feature_dict):
        """
        使用规则在原有价格上调整
        """
        city_id = feature_dict["city_id"]
        build_area = feature_dict["build_size"]
        fitment_code = feature_dict["fitment"]
        fitment_cost = 0
        if fitment_code == "1":
            fitment_cost = self.fitment_rule.get(city_id, 500) * float(build_area)
        new_predict_rlt_lst = []
        for predict_rlt in predict_rlt_details.split("#"):
            predict_rlt = float(predict_rlt) + fitment_cost
            new_predict_rlt_lst.append("%.2f" % predict_rlt)
        new_predict_rlt_details = "#".join(new_predict_rlt_lst)
        return new_predict_rlt_details

    def fix_case(self, predict_rlt, feature_dict):
        target_fix_model = self.target_fix_model_lst

        build_size = float(feature_dict["build_size"])
        base_unit_price = 25000.0
        base_total_price = build_size * base_unit_price

        for model_key in target_fix_model:
            target_predict_rlt = predict_rlt.get(model_key, [])
            fixed_predict_rlt = []
            for idx, each_predict_rlt in enumerate(target_predict_rlt):
                predict_month = each_predict_rlt[0]
                predict_price = each_predict_rlt[1]
                if predict_price > base_total_price:
                    price_diff_rate = float((predict_price - base_total_price)/base_total_price)
                    fix_rate = price_diff_rate / 10.0
                    if fix_rate > 0.05:
                        fix_rate = 0.05
                    predict_price = base_total_price * (1 + fix_rate)
                fixed_predict_rlt.append((predict_month, predict_price))
            predict_rlt[model_key] = fixed_predict_rlt
        return predict_rlt

    def is_match_feature(self, rqst_each):
        '''
        将请求的特征与模型中的特征进行对比
        如果一致,返回1; 不一致,返回0
        '''
        match_rlt_dic = {} # 记录特征匹配信息，记入log
        rqst_feat_dic = {}
        #海波传来的数据,需要对比的项
        rqst_feat_dic["hdic_house_id"] = rqst_each["hdic_house_id"]
        rqst_feat_dic["face_code"] = rqst_each["face_code"]
        rqst_feat_dic["toilet_cnt"] = rqst_each["toilet_amount"]
        rqst_feat_dic["build_size"] = rqst_each["build_size"]
        rqst_feat_dic["floor"] = rqst_each["floor"]
        rqst_feat_dic["total_floor"] = rqst_each["total_floor"]
        rqst_feat_dic["build_end_year"] = rqst_each["build_end_year"]
        rqst_feat_dic["bed_rm_cnt"] = rqst_each["bedroom_amount"]
        rqst_feat_dic["parlor_cnt"] = rqst_each["parlor_amount"]

        #模型中的特征,存放在redis中,进行获取
        redis_info = conf.redis_conn_info
        redis_conn = redis.Redis( host = redis_info["host"], port = redis_info["port"], db = redis_info["db"])

        rqst_key = "feat_" + rqst_feat_dic["hdic_house_id"]
        if redis_conn.exists(rqst_key):
            model_feat = eval(redis_conn.get(rqst_key)) #模型中的特征,格式为json
        #查询不到结果,模型中没有特征,返回0
        else:
            log.warning("no hdic feature in redis\t%s" % rqst_feat_dic["hdic_house_id"])
            return 0
        #将两者特征进行对比,一致返回1,不一致返回0
        for key, val in model_feat.iteritems():
            #对朝向和房屋面积做容错处理
            if key == "face_code":
                if set(val.split(','))==set(rqst_feat_dic[key].split(',')):
                    continue
                else:
                    match_rlt_dic.setdefault(key, {})
                    match_rlt_dic[key]["rqst"] = rqst_feat_dic[key]
                    match_rlt_dic[key]["redis"] = val

            if key == "build_size":
                if round(float(val), 0) == round(float(rqst_feat_dic[key]), 0):
                    continue
                else:
                    match_rlt_dic.setdefault(key, {})
                    match_rlt_dic[key]["rqst"] = rqst_feat_dic[key]
                    match_rlt_dic[key]["redis"] = val

            if val != rqst_feat_dic[key]:
                match_rlt_dic.setdefault(key, {})
                match_rlt_dic[key]["rqst"] = rqst_feat_dic[key]
                match_rlt_dic[key]["redis"] = val

        if len(match_rlt_dic) > 0:
            log.warning("hdic id is, %s, feature not matched is:\t%s" % (rqst_feat_dic["hdic_house_id"], match_rlt_dic))
            return 0
        else:
            return 1


    def is_match_hdic(self, rqst_each):
        '''
        没有用户输入特征时,向楼盘字典返回的价格库中查询估价结果
        '''
        #作为无用户输入的第一个选择入口,返回值添加两个flag,
        # 即hdic是否有数据:hdic_has_data, 以及特征对比是否一致:is_feature_same
        has_hdic_data = 0
        is_feature_same = 0
        resp_dic = dict()

        rqst_data = []
        start = rqst_each["start"]
        end = rqst_each["end"]
        time_type = rqst_each["time_type"]
        hdic_house_id = rqst_each["hdic_house_id"]
        request_id = rqst_each.get("request_id", -1)
        json_param = {"start":start,"end":end,"time_type":time_type,"hdic_house_id":hdic_house_id,"request_id":request_id}

        if time_type == "month":
            json_param = {"start":start[:6],"end":end[:6],"time_type":time_type,"hdic_house_id":hdic_house_id,"request_id":request_id}
        rqst_data.append(json_param)
        url_json = json.JSONEncoder().encode(rqst_data)

        url = 'http://hdic-house-price.search.lianjia.com/hdic_house_price?data=' + url_json

        # 楼盘字典接口发生异常后的处理(两种情况,1.没有价格,返回失败;2.服务器挂机)
        # 加入log日志,记录请求楼盘字典的过程
        try:
            resp_info = eval(urllib.urlopen(url).read())
            resp_dic = resp_info[0] #楼盘字典请求返回结果的字典

            resp_stat = resp_dic.get("rescode", 0)
            # 判断楼盘字典中是否有数据.1表示有价格数据,0表示没有
            if resp_stat == 1:
                is_feature_same = self.is_match_feature(rqst_each)
                has_hdic_data = 1
        except:
            log.warning("[\tlvl=MONITOR\terror=FORMAT\trequest_id=%s\tresp_info=%s\t]", request_id, "hdic server is down !!!")
        finally:
            resp_dic["has_hdic_data"] = has_hdic_data
            resp_dic["is_feature_same"] = is_feature_same
            log.notice("hdic_rqst_info: \t%s\t%s\t%s" % (request_id, json_param, resp_dic))
            return resp_dic

    def has_build_type(self, rqst_each):
        '''
        对build_type进行策略调整
        返回不同build_type对估价的系数
        '''

        build_type = rqst_each.get("build_type",0)
        redis_info = conf.redis_conn_info
        redis_conn = redis.Redis( host = redis_info["host"], port = redis_info["port"], db = redis_info["db"])

        rqst_key = "bld_type_" + rqst_each["resblock_id"]
        if redis_conn.exists(rqst_key):
            type_rlt = eval(redis_conn.get(rqst_key))
            type_cnt = type_rlt["type_cnt"]
            main_type = type_rlt["main_type"]

            if type_cnt == 1:
                return 0
            else:
                if build_type == 0:
                    return self.build_type_dic.get(main_type, 0)
                else:
                    return self.build_type_dic.get(build_type, 0)
        else:
            return 0

    def process_predict_request(self, data):
        '''
        对请求字段进行处理,判断是否进入实时估价模块
        '''
        start_time = time.time()
        rqst_data = json.loads(data)

        check_info_lst = self.check_request_format(rqst_data)
        rescode = 1  # mostly, we use gbdt model
        cur_date = time.strftime('%Y%m%d', time.localtime(time.time()))

        #返回信息
        resp_info = []
        for idx, rqst_each in enumerate(rqst_data):
            #pdb.set_trace()
            each_start_time = time.time()
            request_id = rqst_each.get("request_id", -1)
            has_user_input = rqst_each.get("has_user_input",0) #判断是否有用户输入,该字段没有值则认为没有用户输入
            gujia_type = rqst_each.get("gujia_type", '0')
            check_info = check_info_lst[idx]

            # 如果没有用户输入特征,链接至楼盘字典接口
            # 输出楼盘字典返回的估价值
            go2predict_flag = 0 #没有用户输入时,跳转至实时估价模块的flag

            if has_user_input == '0' and gujia_type == '1':
                resp_tmp = dict()
                hdic_rlt = self.is_match_hdic(rqst_each)
                hdic_has_data = hdic_rlt["has_hdic_data"]
                is_feature_same = hdic_rlt["is_feature_same"]

                # 将李龙数据读取再返回,加日志，李龙接口输入输出
                if hdic_has_data == 1 and is_feature_same == 1:
                    del hdic_rlt["has_hdic_data"]
                    del hdic_rlt["is_feature_same"]
                    resp_tmp["result"] = []
                    for each_hdic_rlt in hdic_rlt["result"]:
                        each_resp_dic = {}
                        each_resp_dic["stat_time"] = str(each_hdic_rlt["stat_date"])
                        each_resp_dic["total_price"] = str(each_hdic_rlt["total_price"])
                        each_resp_dic["max_decr_rate"] = str(each_hdic_rlt["max_decr_rate"])
                        each_resp_dic["max_incr_rate"] = str(each_hdic_rlt["max_incr_rate"])
                        resp_tmp["result"].append(each_resp_dic)

                    resp_tmp["request_id"] = hdic_rlt["request_id"]
                    resp_tmp["rescode"] = hdic_rlt["rescode"]
                    resp_tmp["resmsg"] = 'hdic'
                    resp_info.append(resp_tmp)

                    extra_info_lst = [rqst_each.get("uuid", -1), rqst_each.get("user_id", -1),
                                      rqst_each.get("channel_id", -1), rqst_each.get("city_id", -1), rqst_each.get("os_type", -1)]
                    extra_info_str = "\t".join([str(extra_info) for extra_info in extra_info_lst])
                    hedonic_rlt = []
                    gbdt_rlt = []
                    each_finish_time = time.time()
                    each_time_cost = each_finish_time - each_start_time
                    log.notice("[\trequst=predict\trequest_id=%s\ttime_cost=%.5f\textra_info=%s\thedonic_rlt=%s\tgbdt_rlt=%s\t%s\t"
                               "feature_dict=%s\tresp_info=%s\t]", request_id, each_time_cost, extra_info_str, hedonic_rlt, gbdt_rlt,
                               "-1", rqst_each, str(resp_tmp))  #
                    continue
                else:
                    go2predict_flag = 1

            # 加入对gujia_type的选择
            if has_user_input == '1' or go2predict_flag == 1 or gujia_type != '1':
                #如果请求特征值出错
                if check_info["is_ok"] == False:
                    resp_tmp = dict()
                    resp_tmp["rescode"] = -1
                    resp_tmp["resmsg"] = "fail"
                    resp_tmp["err_info"] = check_info["error"]
                    resp_tmp['request_id'] = request_id
                    log.warning("[\tlvl=MONITOR\terror=FORMAT\trequest_id=%s\trequest_data=%s\tresp_info=%s\t]", request_id, rqst_each, str(resp_tmp))
                    resp_info.append(resp_tmp)
                    continue

                build_type_ratio = float(self.has_build_type(rqst_each))
                time_type = rqst_each["time_type"]
                feature_dict = self.replenish_lost_feature(rqst_each, time_type)
                start = rqst_each["start"]
                end = rqst_each["end"]

                # 按照输入时间段进行预测
                days = (datetime.datetime.strptime(end, "%Y%m%d") - datetime.datetime.strptime(start, "%Y%m%d")).days + 1
                if time_type == "day":
                    input_date_lst = [datetime.datetime.strftime(datetime.datetime.strptime(start, "%Y%m%d") +
                                                                 datetime.timedelta(i), "%Y%m%d") for i in xrange(days)]
                    predict_rlt = self.do_prediction(feature_dict, input_date_lst)
                elif time_type == 'week':
                    weeks = int(days / 7)+1 if days % 7 > 0 else int(days / 7)   # start和end间隔几周
                    input_date_lst = [datetime.datetime.strftime(datetime.datetime.strptime(end, "%Y%m%d") -
                                                                 datetime.timedelta(i*7), "%Y%m%d") for i in xrange(weeks)]
                    input_date_lst.sort()
                    predict_rlt = self.do_prediction(feature_dict, input_date_lst)
                elif time_type == 'month':
                    input_month_lst = pd.period_range(start, end, freq='M')
                    input_date_lst = [item.strftime("%Y%m") for item in input_month_lst]
                    predict_rlt = self.do_prediction(feature_dict, input_date_lst)

                # 对结果进行修正
                resblock_id = feature_dict["resblock_id"]
                bed_rm_cnt = int(feature_dict["bedroom_amount"])
                if resblock_id in ("1111027381756", "1111027381745", "1111027381750") and bed_rm_cnt > 3:  # 处理极端异常的case
                    predict_rlt = self.fix_case(predict_rlt, feature_dict)
                else:
                    predict_rlt = self.price_fix(predict_rlt, feature_dict, request_id, cur_date)  # 根据均价数据对预测结果和均价偏差很大的进行修正

                if time_type == "month":
                    predict_rlt = self.shake_price(predict_rlt, feature_dict, request_id, cur_date[:6])
                else:
                    predict_rlt = self.shake_price(predict_rlt, feature_dict, request_id, cur_date)  # 根据均价过N个月的增长率增强时间敏感度

                if LIST_PRICE_FIX_FLAG:
                    if time_type == "month":
                        predict_rlt = self.list_price_fix(predict_rlt, feature_dict, request_id, cur_date[:6])
                    else:
                        predict_rlt = self.list_price_fix(predict_rlt, feature_dict, request_id, cur_date)

                rescode = predict_rlt["rescode"]
                if rescode != -1:
                    resp_tmp = dict()
                    if rescode == 1:
                        resmsg = "realtime"
                        details = "#".join("%.2f" % rlt[1] for rlt in predict_rlt["gbdt"])

                    details = self.apply_rule(details, feature_dict)
                    details_range = self.get_price_range(details, feature_dict)
                    details_lst = details.split("#")
                    details_range_lst = details_range.split("#")
                    result = [
                        {"total_price": str(float(i)*(1+build_type_ratio)), "stat_time": j, "max_decr_rate": k.split(",")[0], "max_incr_rate": k.split(",")[1]}
                        for (i, j, k) in zip(details_lst, input_date_lst, details_range_lst)
                        ]
                    resp_tmp['result'] = result
                    resp_tmp['resmsg'] = resmsg
                    resp_tmp["rescode"] = rescode
                    resp_tmp['request_id'] = request_id
                elif rescode == -1:
                    resp_tmp = dict()
                    resp_tmp["rescode"] = -1
                    resp_tmp["resmsg"] = "fail"
                    resp_tmp["request_id"] = request_id
                    resp_tmp["err_info"] = "model prediction failed"

                # 日志记录
                each_finish_time = time.time()
                each_time_cost = each_finish_time - each_start_time
                if rescode == -1:
                    log.warning("[\tlvl=MONITOR\terror=MODEL\trequest_id=%s\ttime_cost=%.5f\trequest_data=%s\t"
                                "resp_info=%s\t]", request_id, rqst_each, each_time_cost, str(resp_tmp))
                else:
                    extra_info_lst = [resp_tmp.get("uuid", -1), resp_tmp.get("user_id", -1),
                                      resp_tmp.get("channel_id", -1), resp_tmp.get("city_id", -1), resp_tmp.get("os_type", -1)]
                    extra_info_str = "\t".join([str(extra_info) for extra_info in extra_info_lst])
                    hedonic_rlt = "#".join("%.2f" % rlt for rlt in predict_rlt.get("hedonic", []))
                    gbdt_rlt = "#".join("%.2f" % rlt[1] for rlt in predict_rlt.get("gbdt", []))
                    log.notice("[\trequst=predict\trequest_id=%s\ttime_cost=%.5f\textra_info=%s\thedonic_rlt=%s\tgbdt_rlt=%s\t%s\t"
                               "feature_dict=%s\tresp_info=%s\t]", request_id, each_time_cost, extra_info_str, hedonic_rlt, gbdt_rlt,
                               "-1", rqst_each, str(resp_tmp))  #
                resp_info.append(resp_tmp)

        # 返回前端
        self.write(json.dumps(resp_info))
        finish_time = time.time()
        time_cost = finish_time - start_time
        log.debug("[\ttime_cost=%.5f\trequest_data=%s\tresp_info=%s\t]", time_cost, rqst_data, str(resp_info))

    def process_feedback_request(self, data):
        """
        负责记录用户的反馈信息
        """
        start_time = time.time()
        rqst_data = eval(data)
        request_id = rqst_data.get("request_id", -1)
        missed_info = []
        for important_info_key in ["request_id", "uuid", "user_id", "feedback", "feedback_time"]:
            if important_info_key not in rqst_data:
                missed_info.append(important_info_key)
        if len(missed_info) == 0:
            fb_lst = [ rqst_data["uuid"], rqst_data["user_id"],\
                       rqst_data["feedback"], rqst_data["feedback_time"] ]
            fb_str = "\t".join([str(fb) for fb in fb_lst])
            resp_info = '{"rescode":0,"resmsg":"成功"}'
            self.write(resp_info)
            time_cost = time.time() - start_time

        else:
            resp_info = '{"rescode":-1,"resmsg":"fail","err_info": "missed_info: %s"}' % (",".join(missed_info))
            self.write(resp_info)
            time_cost = time.time() - start_time

    def get(self):
        try:
            rqst_type = self.get_argument("request_type")
            data = self.get_argument("data")
            request_type = '';
            if(rqst_type == '0'):
                request_type = 'price_predict'
            elif(rqst_type == '1'):
                request_type = 'feedback'
            if rqst_type == "0":
                self.process_predict_request(data)
            elif rqst_type == "1":
                self.process_feedback_request(data)
        except Exception, e:
            traceback.print_exc()

    def post(self):
        try:
            rqst_type = self.get_argument("request_type")
            data = self.get_argument("data")
            request_type = ''
            if(rqst_type == '0'):
                request_type = 'price_predict'
            elif(rqst_type == '1'):
                request_type = 'feedback'
            if rqst_type == "0":
                self.process_predict_request(data)
            elif rqst_type == "1":
                self.process_feedback_request(data)
        except Exception, e:
            traceback.print_exc()


def run_server():
    application = tornado.web.Application([(r"/", MainHandler)])
    port = int(sys.argv[1])
    application.listen(port)
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    run_server()
