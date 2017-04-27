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

    def is_match_hdic(self):
        '''
        没有用户输入特征时,向楼盘字典返回的价格库中查询估价结果
        '''
        pass

    def process_predict_request(self, data):
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

            if has_user_input == '0':
                self.is_match_hdic()
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
