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

PRICE_FIX_FLAG = conf.PRICE_FIX_FLAG  # 1:fix; 0:no fix
PRICE_FIX_THRESHOLD = conf.PRICE_FIX_THRESHOLD
PENALTY_FACTOR = conf.PENALTY_FACTOR
UPDATE_INTERVAL = conf.UPDATE_INTERVAL
UPDATE_LOSS_RATE = conf.UPDATE_LOSS_RATE

AVG_PRICE_MONTH_CNT = conf.AVG_PRICE_MONTH_CNT  # 加载月均价近N个月的数据
AVG_PRICE_MONTH_CNT_DAY = conf.AVG_PRICE_MONTH_CNT_DAY  # 加载日均价近N个月的数据
MAX_INCR_RATE = conf.MAX_INCR_RATE
MAX_DECR_RATE = conf.MAX_DECR_RATE


class PreparedData:

    def __init__(self):
        self.max_incr_rate = MAX_INCR_RATE
        self.max_decr_rate = MAX_DECR_RATE
        self.gbdt_model_dict = self.load_model("GBDT", conf.GBDT_MODEL_DIR)  # {'bizcircle_code': model}
        #self.hedonic_model_dict = self.load_model("HEDONIC", conf.HEDONIC_MODEL_DIR)  # {'resblock_id': model}
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
#hedonic_model_dict = prepared_data.hedonic_model_dict
adjust_info2rate = prepared_data.adjust_info2rate
key2price_range = prepared_data.key2price_range
last_update_time = time.time()

class MainHandler(tornado.web.RequestHandler):

    def initialize(self):
        """
        初始化模型和日均价数据
        """
        self.gbdt_model_dict = gbdt_model_dict
        #self.hedonic_model_dict = hedonic_model_dict
        self.resblock2avg_price_day = resblock2avg_price_day
        self.resblock2avg_listprice_day = resblock2avg_listprice_day
        self.resblock2avg_incr_rate_day = resblock2avg_incr_rate_day
        self.important_ftr_lst = conf.IMPORTANT_FEATURE
        self.feature_lst = conf.FEATURE_LIST
        self.default_month_shift_lst = conf.DEFAULT_MONTH_SHIFT_LST
        self.target_shake_model_lst = conf.TARGET_SHAKE_MODEL_LST
        self.target_fix_model_lst = conf.TARGET_FIX_MODEL_LST
        self.fitment_rule = conf.FITMENT_RULE
        self.stablize_day_cnt = conf.stablize_day_cnt
        self.force_reasonable = conf.FORCE_REASONABLE
        self.model_dim = conf.MODEL_DIM

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
        if feature in ["fitment", "is_sales_tax", "is_sole", "is_school_district"]:
            if not feature_value.isdigit():
                return False
            if eval(feature_value) not in [0, 1]:
                return False
            else:
                return True
        return True

    def check_request_format(self, rqst_data):
        """
        检查必须的特征是否传过来，检查指定特征的值域是否符合要求
        """

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
            if feature_name == "frame_structure":
                feature_dict[feature_name] = str(checked_rqst_data.get(feature_name, "null"))
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
        if input_target_time == "": #default,用于兼容老版本
            idx2target_date = self.generate_target_date()
            for shift in self.default_month_shift_lst:
                target_date = idx2target_date[shift]
                input_target_day.append(target_date)
        else:
            if len(input_target_time[0] == 6):
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

                # 学习写法!!
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
                    test_tag_lst = ["is_sales_tax", "is_school_district", "is_sole"]
                    for test_tag in test_tag_lst:
                        tmp_feature_dict = copy.copy(feature_dict)
                        tmp_feature_dict["is_sales_tax"] = '0'
                        tmp_feature_dict["is_school_district"] = '0'
                        tmp_feature_dict["is_sole"] = '0'

                        if test_tag == "is_sales_tax":
                            tmp_feature_dict["is_sales_tax"] = '0'
                            tmp_is_sales_tax_0_rlt = target_gbdt_model.predict(tmp_feature_dict)
                            tmp_feature_dict["is_sales_tax"] = '1'
                            tmp_is_sales_tax_1_rlt = target_gbdt_model.predict(tmp_feature_dict)
                            is_sales_tax_0_rlt = min(tmp_is_sales_tax_0_rlt, tmp_is_sales_tax_1_rlt)
                            is_sales_tax_1_rlt = max(tmp_is_sales_tax_0_rlt, tmp_is_sales_tax_1_rlt)
                        elif test_tag == "is_school_district":
                            tmp_feature_dict["is_school_district"] = '0'
                            tmp_is_school_district_0_rlt = target_gbdt_model.predict(tmp_feature_dict)
                            tmp_feature_dict["is_school_district"] = '1'
                            tmp_is_school_district_1_rlt = target_gbdt_model.predict(tmp_feature_dict)
                            is_school_district_0_rlt = min(tmp_is_school_district_0_rlt, tmp_is_school_district_1_rlt)
                            is_school_district_1_rlt = max(tmp_is_school_district_0_rlt, tmp_is_school_district_1_rlt)

                        elif test_tag == "is_sole":
                            tmp_feature_dict["is_sole"] = '0'
                            tmp_is_sole_0_rlt = target_gbdt_model.predict(tmp_feature_dict)
                            tmp_feature_dict["is_sole"] = '1'
                            tmp_is_sole_1_rlt = target_gbdt_model.predict(tmp_feature_dict)
                            is_sole_0_rlt = min(tmp_is_sole_0_rlt, tmp_is_sole_1_rlt)
                            is_sole_1_rlt = max(tmp_is_sole_0_rlt, tmp_is_sole_1_rlt)

                    #判断传入特征中是否包含这三个
                    is_sales_tax_rlt = is_sales_tax_1_rlt if feature_dict["is_sales_tax"] == '1' else is_sales_tax_0_rlt
                    is_school_district_rlt = is_school_district_1_rlt if feature_dict["is_school_district"] == '1' else is_school_district_0_rlt
                    is_sole_rlt = is_sole_1_rlt if feature_dict["is_sole"] == '1' else is_sole_0_rlt
                    gbdt_predict_rlt = (is_sales_tax_rlt + is_school_district_rlt + is_sole_rlt) / 3.0
                else:
                    gbdt_predict_rlt = target_gbdt_model.predict(feature_dict)
                predict_rlt["gbdt"].append((target_date, gbdt_predict_rlt))

        if len(predict_rlt.get("gbdt", [])) != 0:
            predict_rlt["rescode"] = 1
        else:
            predict_rlt["rescode"] = -1
        return predict_rlt

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

    def price_fix(self, predict_rlt, feature_dict, request_id, cur_date):
        #对估价原始值过低的预测进行基于小区均价的规则修正
        target_fix_model = self.target_fix_model_lst
        build_size = float(feature_dict["build_size"])
        for model_key in target_fix_model:
            target_predict_rlt = predict_rlt.get(model_key, [])
            fixed_predict_rlt = []
            for idx, each_predict_rlt in enumerate(target_predict_rlt):
                predict_month = each_predict_rlt[0]
                predict_price = each_predict_rlt[1]
                if predict_month in feature_dict["resblock_trans_price"]:
                    resblock_avg_price = feature_dict["resblock_trans_price"][predict_month]["-1"]
                else: #default
                    resblock_avg_price = feature_dict["resblock_trans_price"]["latest_date"]["-1"]
                avg_total_price = build_size * resblock_avg_price
                price_diff = predict_price - avg_total_price
                diff_rate = abs(price_diff) / avg_total_price
                old_price = float(predict_price)
                if diff_rate > PRICE_FIX_THRESHOLD and price_diff < 0:
                    predict_price += abs(price_diff) * PENALTY_FACTOR
                    log.notice("price_fix\t%s\t%s\t%s" % (request_id, old_price, float(predict_price)))
                if diff_rate > PRICE_FIX_THRESHOLD and price_diff > 0:
                    predict_price -= abs(price_diff) * PENALTY_FACTOR
                    log.notice("price_fix\t%s\t%s\t%s" % (request_id, old_price, float(predict_price)))
                predict_price = self.adjust_price(predict_price, feature_dict)
                fixed_predict_rlt.append((predict_month, predict_price))
            predict_rlt[model_key] = fixed_predict_rlt
        return predict_rlt

    def shake_price(self, predict_rlt, feature_dict, request_id, cur_date):
        #根据挂牌价近期走势，对估价结果在时间维度上微调
        target_shake_model = self.target_shake_model_lst
        cur_month = cur_date[:6]
        for model_key in target_shake_model:
            target_predict_rlt = predict_rlt.get(model_key, [])
            shaked_predict_rlt = []
            for idx, each_rlt in enumerate(target_predict_rlt):
                predict_month = each_rlt[0]
                predict_price = each_rlt[1]
                last_idx = idx - 1
                if idx == 0 or predict_month == cur_month:
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
                    log.notice("price_shake\t%s\t%s\t%s" % (request_id, float(predict_price), float(new_predict_price)))
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
            min_rlt = float(predict_rlt) * (1 + range_info[0])
            max_rlt = float(predict_rlt) * (1 + range_info[1])
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

    def is_match_feature(self, feature_lst):
        pass

    def is_match_hdic(self, rqst_each):
        '''
        没有用户输入特征时,向楼盘字典返回的价格库中查询估价结果
        '''
        pass
        #作为无用户输入的第一个选择入口,返回值添加两个flag,
        # 即hdic是否有数据:hdic_has_data, 以及特征对比是否一致:is_feature_same
        hdic_has_data = 0
        is_feature_same = 0

        rqst_data = []
        start = rqst_each["start"]
        end = rqst_each["end"]
        time_type = rqst_each["time_type"]
        hdic_house_id = rqst_each["hdic_house_id"]
        request_id = rqst_each.get("request_id", -1)
        json_param = {"start":start,"end":end,"time_type":time_type,"hdic_house_id":hdic_house_id,"request_id":request_id}
        rqst_data.append(json_param)
        url_json = json.JSONEncoder().encode(rqst_data)
        url = 'http://172.16.5.21:3939/hdic_house_price?data=' + url_json
        resp_info = eval(urllib.urlopen(url).read())
        resp_dic = resp_info[0] #楼盘字典请求返回结果的字典

        resp_stat = resp_dic["rescode"]
        # 判断楼盘字典中是否有数据.1表示有价格数据,0表示没有
        if resp_stat == 1:
            is_feature_same = self.is_match_feature(rqst_each)

        resp_dic["hdic_has_data"] = hdic_has_data
        resp_dic["is_feature_same"] = is_feature_same
        return resp_stat


    def process_predict_request(self, data):
        '''
        对请求字段进行处理,判断是否进入实时估价模块
        '''
        start_time = time.time()
        rqst_data = json.loads(data)

        check_info_lst = self.check_request_format(rqst_data)
        rescode = 1  # mostly, we use gbdt model
        cur_time = time.time()
        cur_date = time.strftime('%Y%m%d', time.localtime(time.time()))

        #返回信息
        resp_info = []
        for idx, rqst_each in enumerate(rqst_data):
            request_id = rqst_each.get("request_id", -1)
            has_user_input = rqst_each.get("has_user_input",0) #判断是否有用户输入,该字段没有值则认为没有用户输入
            check_info = check_info_lst[idx]

            #如果没有用户输入特征,链接至楼盘字典接口
            if has_user_input == '0':
                hdic_rlt = self.is_match_hdic(rqst_each)
                hdic_has_data = hdic_rlt["hdic_has_data"]
                is_feature_same = hdic_rlt["is_feature_same"]
            else:
                #有用户输入特征,进入实时估计模块

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
                    input_date_lst = self.get_month_lst(start, end) # 将时间列表截为6位, 例如: [201704, 201705]
                    input_month_lst = input_date_lst.sort()
                    # input_month_str = ",".join(lst for lst in input_date_lst)
                    predict_rlt = self.do_prediction(feature_dict, input_month_lst)

                # 对结果进行修正
                resblock_id = feature_dict["resblock_id"]
                bed_rm_cnt = int(feature_dict["bedroom_amount"])
                if resblock_id in ("1111027381756", "1111027381745", "1111027381750") and bed_rm_cnt > 3:  # 处理极端异常的case
                    predict_rlt = self.fix_case(predict_rlt, feature_dict)
                else:
                    predict_rlt = self.price_fix(predict_rlt, feature_dict, request_id, cur_date)  # 根据均价数据对预测结果和均价偏差很大的进行修正
                # predict_rlt = self.shake_price(predict_rlt, feature_dict, request_id, cur_date)  # 根据均价过N个月的增长率增强时间敏感度
                rescode = predict_rlt["rescode"]
                if rescode != -1:
                    resp_tmp = dict()
                    if rescode == 1:
                        resmsg = "use GBDT model"
                        details = "#".join("%.2f" % rlt[1] for rlt in predict_rlt["gbdt"])

                    details = self.apply_rule(details, feature_dict)

                    details_lst = details.split("#")
                    result = [{"total_price": i, time_type: j} for (i, j) in zip(details_lst, input_date_lst)]
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
                if rescode == -1:
                    log.warning("[\tlvl=MONITOR\terror=MODEL\trequest_id=%s\ttime_cost=%.5f\trequest_data=%s\t"
                                "resp_info=%s\t]", request_id, rqst_each, -1, str(resp_tmp))
                else:
                    extra_info_lst = [resp_tmp.get("uuid", -1), resp_tmp.get("user_id", -1),
                                      resp_tmp.get("channel_id", -1), resp_tmp.get("city_id", -1), resp_tmp.get("os_type", -1)]
                    extra_info_str = "\t".join([str(extra_info) for extra_info in extra_info_lst])
                    hedonic_rlt = "#".join("%.2f" % rlt for rlt in predict_rlt.get("hedonic", []))
                    gbdt_rlt = "#".join("%.2f" % rlt[1] for rlt in predict_rlt.get("gbdt", []))
                    log.notice("[\trequst=predict\trequest_id=%s\ttime_cost=%.5f\textra_info=%s\thedonic_rlt=%s\tgbdt_rlt=%s\t%s\t"
                               "feature_dict=%s\tresp_info=%s\t]", request_id, -1, extra_info_str, hedonic_rlt, gbdt_rlt,
                               feature_dict, rqst_each, str(resp_tmp))  #
                resp_info.append(resp_tmp)

        # 返回前端
        self.write(json.dumps(resp_info))
        finish_time = time.time()
        time_cost = finish_time - start_time
        log.debug("[\ttime_cost=%.5f\trequest_data=%s\tresp_info=%s\t]", time_cost, rqst_data, str(resp_info))


        #mock接口逻辑
        # rqst_data = eval(data)
        # response_lst = []
        # for one_data in rqst_data:
        #     request_id = one_data.get("request_id", -1)
        #     time_type = one_data.get("time_type")
        #     resp_date_lst = []
        #     resp_dict = {"request_id": request_id, "rescode": 1, "resmsg": "success", "recent_update": 0}
        #
        #     resp_dict.setdefault("result", [])
        #
        #     demo_dic = {"total_price":"1234567.89",
        #                 "max_decr_rate":"0.05",
        #                 "max_incr_rate":"0.05"}
        #     if time_type=="day":
        #         date_flag = 'D'
        #         data_strf = '%Y%m%d'
        #     elif time_type=="month":
        #         date_flag = 'M'
        #         data_strf = '%Y%m'
        #     start_time = datetime.strptime(one_data.get("start"),'%Y%m%d')
        #     end_time = datetime.strptime(one_data.get("end"),'%Y%m%d')
        #     resp_date_lst = pd.period_range(start_time, end_time, freq=date_flag)
        #     print resp_date_lst
        #
        #     for item in resp_date_lst:
        #         result_dic = {"total_price":"1234567.89","max_decr_rate":"0.05","max_incr_rate":"0.05"}
        #         result_dic["stat_time"] = item.strftime(data_strf)
        #         resp_dict["result"].append(result_dic)
        #     response_lst.append(resp_dict)
        # self.write(json.dumps(response_lst))


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
                print("GET Method")
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
                print("POST Method")
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
