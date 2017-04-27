#coding: utf-8
__author__ = 'Murphy'

import tornado.ioloop
import tornado.web

import datetime
import os
import sys
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

sys.path.append("conf")
sys.path.append("lib")
import log
import conf as conf
import calendar

import pandas as pd
import json
import traceback
import pdb

gbdt_model_dict = prepared_data.gbdt_model_dict
hedonic_model_dict = prepared_data.hedonic_model_dict
adjust_info2rate = prepared_data.adjust_info2rate
key2price_range = prepared_data.key2price_range
last_update_time = time.time()

class MainHandler(tornado.web.RequestHandler):

    def initialize(self):
        """
        初始化模型和日均价数据
        """
        self.gbdt_model_dict = gbdt_model_dict
        self.hedonic_model_dict = hedonic_model_dict
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

    def do_prediction(self, feature_dict, input_target_day):
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

        model_key = bizcircle_code
        if self.model_dim == "district":
            model_key = district_id

        if model_key in self.gbdt_model_dict:
            target_gbdt_model = self.gbdt_model_dict[model_key]
            for target_date in input_target_day:
                feature_dict["dealdate"] = target_date   # 20161207格式
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

    def is_match_hdic(self):
        '''
        没有用户输入特征时,向楼盘字典返回的价格库中查询估价结果
        '''
        pass

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
                self.is_match_hdic()
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
                    input_date_lst = self.get_month_lst(start, end)
                    input_date_lst.sort()
                    input_month_str = ",".join(lst for lst in input_date_lst)
                    predict_rlt = super(BranchHandler, self).do_prediction(feature_dict, input_month_str)

                # 对结果进行修正
                resblock_id = feature_dict["resblock_id"]
                bed_rm_cnt = int(feature_dict["bedroom_amount"])
                if resblock_id in ("1111027381756", "1111027381745", "1111027381750") and bed_rm_cnt > 3:  # 处理极端异常的case
                    predict_rlt = super(BranchHandler, self).fix_case(predict_rlt, feature_dict)
                else:
                    predict_rlt = super(BranchHandler, self).price_fix(predict_rlt, feature_dict, request_id, cur_date)  # 根据均价数据对预测结果和均价偏差很大的进行修正
                # predict_rlt = self.shake_price(predict_rlt, feature_dict, request_id, cur_date)  # 根据均价过N个月的增长率增强时间敏感度
                rescode = predict_rlt["rescode"]
                if rescode != -1:
                    resp_tmp = dict()
                    if rescode == 0 and False:  # 暂时弃用
                        resmsg = "use HEDONIC model"
                        details = "#".join("%.2f" % rlt for rlt in predict_rlt["hedonic"])
                    elif rescode == 1:
                        resmsg = "use GBDT model"
                        details = "#".join("%.2f" % rlt[1] for rlt in predict_rlt["gbdt"])

                    details = super(BranchHandler, self).apply_rule(details, feature_dict)
                    if conf.stablize_flag:
                        try:
                            details = super(BranchHandler, self).stablize_rlt(details, feature_dict, request_id)
                        except Exception, e:
                            log.warning("%s: stablize err: %s, %s" % (request_id, traceback.format_exc(), e))
                    # details_range = self.get_price_range(details, feature_dict)
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
