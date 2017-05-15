#!/bin/python
# coding: utf8

from __future__ import division
import sys
import numpy as np
import feature_method
from sklearn import ensemble
from sklearn.externals import joblib
sys.path.append('lib')
sys.path.append('conf')
import log
import math
import pdb

def str2float(item):
    if type(item) is str:
        return float(item)
    elif type(item) is float:
        return item
    else:
        raise ValueError("str2float Error")


class PriceModel:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.filename = 'model/'
        self.model = None
        if PriceModel.__is_gbdt__(name):
            self.filename = 'model/gbdt/gbdt.' + id + '.model'
            self.model = joblib.load(self.filename)
        else:
            raise ValueError("Model name only should in [GBDT, gbdt]")

    @staticmethod
    def __is_gbdt__(name):
        if name in ('gbdt', 'GBDT'):
            return True
        else:
            return False

    def __X_to_x__(self, X):
        """
        transform dict X to nparray x for gbdt
        """
        if PriceModel.__is_gbdt__(self.name):
            floor = feature_method.get_floor(X["floor"])
            total_floor = feature_method.get_total_floor(X["total_floor"])
            # 临时将fitment写死为2：简装
            x = [
                    str2float(X["bedroom_amount"]),
                    str2float(X["parlor_amount"]),
                    str2float(X["toilet_amount"]),
                    str2float(X["build_size"]),
                    feature_method.get_face_new(X["face_code"]),
                    feature_method.get_build_end_year_fromnow(X["build_end_year"]),
                    feature_method.get_dealdate_fromnow(X["dealdate"]),
                    str2float(str(X["is_five"])),
                    str2float(str(X["is_sole"])),
                    feature_method.get_is_school_district(str(X["max_school_level"])),
                    feature_method.get_distance_metro_code(X["distance_metor"]),
                    total_floor,
                    floor,
                    feature_method.get_floor_total_floor_scale(floor, total_floor),
                    str2float(X["resblock_trans_price_comm"]),
                    str2float(X["resblock_trans_price_room"]),
                    str2float(X["resblock_trans_list_avg_room"]),
                    str2float(X["trans_total_price_comm"]),
                    str2float(X["list_total_price_comm"]),
                    str2float(X["trans_total_price_room"]),
                    str2float(X["list_total_price_room"]),
                    str2float(X["trans_list_total_price_room"])
                ]
            '''
            pdb.set_trace()
            str1 = "resblock_id: %s\n" % X["resblock_id"]
            str1 += "bedroom_amount:%f\n" %str2float(X["bedroom_amount"])
            str1 += "parlor_amount:%f\n"  %str2float(X["parlor_amount"])
            str1 +=  "toilet_amount:%f\n" %str2float(X["toilet_amount"])
            #str += "cookroom_amount:%f\n"  %str2float(X["cookroom_amount"])
            str1 +=  "build_size:%f\n"  %str2float(X["build_size"])
            str1 +=  "face_code:%f\n"  %feature_method.get_face_new(X["face_code"])
            str1 +=  "build_end_year:%f\n" %feature_method.get_build_end_year_fromnow(X["build_end_year"])
            #str +=  "fitment:%f\n"  %str2float(X["fitment"])
            str1 +=  "dealdate:%f\n"  %feature_method.get_dealdate_fromnow(X["dealdate"])
            #str += "property_fee:%f\n" %get_property_fee(X["property_fee"])
            str1 +=  "is_sales_tax:%f\n" %str2float(X["is_five"])
            str1 +=  "is_sole:%f\n" %str2float(X["is_sole"])
            str1 +=  "is_school_district:%f\n"  %feature_method.get_is_school_district(X["max_school_level"])
            str1 +=  "distance_metor:%f\n" %feature_method.get_distance_metro_code(X["distance_metor"])
            str1 +=  "total_floor:%f\n" %total_floor
            str1 +=  "floor:%f\n" %floor
            str1 +=  "floor_scale:%f\n" %feature_method.get_floor_total_floor_scale(floor,total_floor)
            #str += "balcony_amount:%f\n" %get_balcony_amount(X["balcony_amount"])
            #str1 +=  "frame_structure:%f\n" %feature_method.get_frame_structure(X["frame_structure"])
            #str += "garden_amount:%f\n" %get_garden_amount(X["garden_amount"])
            #str += "terrace_amount:%f\n" %get_terrace_amount(X["terrace_amount"])
            str1 += "resblock_trans_price_comm:%.3f\n" % str2float(X["resblock_trans_price_comm"])
            str1 += "resblock_trans_price_room:%.3f\n" % str2float(X["resblock_trans_price_room"])
            str1 += "resblock_trans_list_avg_room:%.3f\n" % str2float(X["resblock_trans_list_avg_room"])
            str1 += "trans_total_price_comm:%.3f\n" % str2float(X["trans_total_price_comm"])
            str1 += "list_total_price_comm:%.3f\n" % str2float(X["list_total_price_comm"])
            str1 += "trans_total_price_room:%.3f\n" % str2float(X["trans_total_price_room"])
            str1 += "list_total_price_room:%.3f\n" % str2float(X["list_total_price_room"])
            str1 += "trans_list_total_price_room:%.3f\n" % str2float(X["trans_list_total_price_room"])

            log.notice("%s", str1)
            '''
            return np.array(x)
        else:
            raise ValueError("Not Ready for Hedonic......")
    
    def predict(self, X):
        return self.model.predict(self.__X_to_x__(X))
     
if __name__ == '__main__':
    model = PriceModel('gbdt', '611100321')
    X = {}
    X["resblock_id"] = "test_block"
    X["bizcircle_code"] = "test"
    X["bedroom_amount"] = "3"
    X["parlor_amount"] = "1"
    X["toilet_amount"] = "1"
    # X["cookroom_amount"] = "0"
    X["build_size"] = "91"
    X["face_code"] = "100500000003|100500000007"
    X["build_end_year"] = "2012"
    # X["fitment"] = "1"
    X["dealdate"] = "20150929"
    # X["property_fee"] = "0"
    X["is_five"] = "1"
    X["is_sole"] = "1"
    X["max_school_level"] = "1"
    X["distance_metor"] = "900"
    X["total_floor"] = "28"
    X["floor"] = "6"
    # X["balcony_amount"] = "0"
    X["frame_structure"] = "304400000001"
    # X["garden_amount"] = "0"
    # X["terrace_amount"] = "0"
    X["resblock_trans_price"] = "45341"
    ret = model.predict(X)
    print(ret[0])
