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

class __Hedonic_Model__:
    def __init__(self, filename):
        hmodel = {}
        hmodel['(Intercept)'] = 0
        hmodel['floor_ftr'] = 0
        hmodel['bedroom_amount'] = 0
        hmodel['parlor_amount'] = 0
        hmodel['toilet_amount'] = 0
        hmodel['cookroom_amount'] = 0
        hmodel['build_size'] = 0
        hmodel['face_ftr'] = 0
        hmodel['face_ftr_new'] = 0
        hmodel['build_end_year_ftr'] = 0
        hmodel['build_end_year_ftr_new'] = 0
        hmodel['is_sales_tax_ftr'] = 0
        hmodel['is_sole_ftr'] = 0
        hmodel['property_fee_ptr'] = 0
        hmodel['distance_metor_code_ftr'] = 0
        hmodel['is_school_district_ftr'] = 0
        hmodel['fitment_ftr'] = 0
        hmodel['total_floor_ftr'] = 0
        hmodel['floor_scale'] = 0
        hmodel['dealdate_ftr'] = 0
        hmodel['dealdate_ftr_new'] = 0
        hmodel['scale'] = 0
        hmodel['balcony_amount_ftr'] = 0
        hmodel['frame_structure_ftr'] = 0
        hmodel['garden_amount_ftr'] = 0
        hmodel['terrace_amount_ftr'] = 0
        hmodel['balcony_area_ftr'] = 0
        hmodel['resblock_trans_price'] = 0
        
        hmodel['log((Intercept))'] = 0
        hmodel['log(floor_ftr)'] = 0
        hmodel['log(bedroom_amount)'] = 0
        hmodel['log(parlor_amount)'] = 0
        hmodel['log(toilet_amount)'] = 0
        hmodel['log(cookroom_amount)'] = 0
        hmodel['log(build_size)'] = 0
        hmodel['log(face_ftr)'] = 0
        hmodel['log(face_ftr_new)'] = 0
        hmodel['log(build_end_year_ftr)'] = 0
        hmodel['log(build_end_year_ftr_new)'] = 0
        hmodel['log(is_sales_tax_ftr)'] = 0
        hmodel['log(is_sole_ftr)'] = 0
        hmodel['log(property_fee_ptr)'] = 0
        hmodel['log(distance_metor_code_ftr)'] = 0
        hmodel['log(is_school_district_ftr)'] = 0
        hmodel['log(fitment_ftr)'] = 0
        hmodel['log(total_floor_ftr)'] = 0
        hmodel['log(floor_scale)'] = 0
        hmodel['log(dealdate_ftr)'] = 0
        hmodel['log(dealdate_ftr_new)'] = 0
        hmodel['log(scale)'] = 0
        hmodel['log(balcony_amount_ftr)'] = 0
        hmodel['log(frame_structure_ftr)'] = 0
        hmodel['log(garden_amount_ftr)'] = 0
        hmodel['log(terrace_amount_ftr)'] = 0
        hmodel['log(balcony_area_ftr)'] = 0
        hmodel['log(resblock_trans_price)'] = 0
        
        for line in open(filename):
            string = line.strip()
            model = eval(string)
            for key in model:
                if key in hmodel:
                    hmodel[key] = float(model[key])
                else:
                    log.debug("[\tkey=%s\t]key not in model", key)
            break
        self.model = hmodel

    def __feature_map__(self, fname):
        fnames = {
            "face_code": ["face_ftr", "face_ftr_new"],
            "build_end_year": ["build_end_year_ftr", "build_end_year_ftr_new"],
            "fitment": ["fitment_ftr"],
            "dealdate": ["dealdate_ftr", "dealdate_ftr_new"],
            "property_fee": ["property_fee_ptr"],
            "is_sales_tax": ["is_sales_tax_ftr"],
            "is_sole": ["is_sole_ftr"],
            "is_school_district": ["is_school_district_ftr"],
            "distance_metor": ["distance_metor_code_ftr"],
            "total_floor": ["total_floor_ftr"],
            "floor": ["floor_ftr"], 
            "floor_scale": ["floor_scale"],
            "balcony_amount": ["balcony_amount_ftr"],
            "frame_structure": ["frame_structure_ftr"],
            "garden_amount": ["garden_amount_ftr"],
            "terrace_amount": ["terrace_amount_ftr"]
        }
        if(fname in fnames):
            return fnames[fname]
        else:
            return [fname]

    def __X_to_x__(self,X):
        x = {}
        floor = get_floor(X["floor"])
        total_floor = get_total_floor(X["total_floor"])
        x = {\
                "(Intercept)":1,\
                "bedroom_amount":str2float(X["bedroom_amount"]),\
                "parlor_amount":str2float(X["parlor_amount"]),\
                "toilet_amount":str2float(X["toilet_amount"]),\
                "cookroom_amount":str2float(X["cookroom_amount"]),\
                "build_size":str2float(X["build_size"]),\
                "face_code":get_face_new(X["face_code"]),\
                "build_end_year":get_build_end_year_fromnow(X["build_end_year"]),\
                "fitment":str2float(X["fitment"]),\
                "dealdate":get_dealdate_fromnow(X["dealdate"]),\
                "property_fee":get_property_fee(X["property_fee"]),\
                "is_sales_tax":str2float(X["is_sales_tax"]),\
                "is_sole":str2float(X["is_sole"]),\
                "is_school_district":get_is_school_district(X["is_school_district"]),\
                "distance_metor":get_distance_metro_code(X["distance_metor"]),\
                "total_floor":total_floor,\
                "floor":floor,\
                "floor_scale":get_floor_total_floor_scale(floor,total_floor),\
                "balcony_amount":get_balcony_amount(X["balcony_amount"]),\
                "frame_structure":get_frame_structure(X["frame_structure"]),\
                "garden_amount":get_garden_amount(X["garden_amount"]),\
                "terrace_amount":get_terrace_amount(X["terrace_amount"]),\
                "resblock_trans_price":str2float(X["resblock_trans_price"])\
        }
        return x
       
    def predict(self, X):
        x = self.__X_to_x__(X)
        ret = 0.0
        for key in x:
            v = float(x[key])
            fns = self.__feature_map__(key)
            for fn in fns:
                ret += v * self.model[fn]
                if(v > 0):
                    ret += math.log(v) * self.model['log(' + fn + ')']
        return np.array([math.exp(ret)])


class PriceModel:
    def __init__(self, name, id):
        self.name = name
        self.id = id
        self.filename = 'model/'
        self.model = None
        if PriceModel.__is_gbdt__(name):
            self.filename = 'model/gbdt/gbdt.' + id + '.model'
            self.model = joblib.load(self.filename)
        elif PriceModel.__is_hedonic__(name):
            self.filename = 'model/hedonic/' + id + '.model'
            self.model = __Hedonic_Model__(self.filename)
        else:
            raise ValueError("Model name only should in [GBDT, gbdt, hedonic, Hedonic, HEDONIC]")

    @staticmethod
    def __is_hedonic__(name):
        if name in ('hedonic', 'HEDONIC', 'Hedonic'):
            return True
        else:
            return False
            
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
                    str2float(X["is_sales_tax"]),
                    str2float(X["is_sole"]),
                    feature_method.get_is_school_district(X["is_school_district"]),
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
            """
            str = "bizcircle_code:%s\n" % X["bizcircle_code"]
            str += "resblock_id: %s\n" % X["resblock_id"]
            str += "bedroom_amount:%f\n" %str2float(X["bedroom_amount"])
            str += "parlor_amount:%f\n"  %str2float(X["parlor_amount"])
            str +=  "toilet_amount:%f\n" %str2float(X["toilet_amount"])
            #str += "cookroom_amount:%f\n"  %str2float(X["cookroom_amount"])
            str +=  "build_size:%f\n"  %str2float(X["build_size"])
            str +=  "face_code:%f\n"  %get_face_new(X["face_code"])
            str +=  "build_end_year:%f\n" %get_build_end_year_fromnow(X["build_end_year"])
            #str +=  "fitment:%f\n"  %str2float(X["fitment"])
            str +=  "dealdate:%f\n"  %get_dealdate_fromnow(X["dealdate"])
            #str += "property_fee:%f\n" %get_property_fee(X["property_fee"])
            str +=  "is_sales_tax:%f\n" %str2float(X["is_sales_tax"])
            str +=  "is_sole:%f\n" %str2float(X["is_sole"])
            str +=  "is_school_district:%f\n"  %get_is_school_district(X["is_school_district"])
            str +=  "distance_metor:%f\n" %get_distance_metro_code(X["distance_metor"])
            str +=  "total_floor:%f\n" %total_floor
            str +=  "floor:%f\n" %floor
            str +=  "floor_scale:%f\n" %get_floor_total_floor_scale(floor,total_floor)
            #str += "balcony_amount:%f\n" %get_balcony_amount(X["balcony_amount"])
            str +=  "frame_structure:%f\n" %get_frame_structure(X["frame_structure"])
            #str += "garden_amount:%f\n" %get_garden_amount(X["garden_amount"])
            #str += "terrace_amount:%f\n" %get_terrace_amount(X["terrace_amount"])
            str += "resblock_trans_price_comm:%.3f\n" % str2float(X["resblock_trans_price_comm"])
            str += "resblock_trans_price_room:%.3f\n" % str2float(X["resblock_trans_price_room"])
            str += "resblock_trans_list_avg_room:%.3f\n" % str2float(X["resblock_trans_list_avg_room"])
            str += "trans_total_price_comm:%.3f\n" % str2float(X["trans_total_price_comm"])
            str += "list_total_price_comm:%.3f\n" % str2float(X["list_total_price_comm"])
            str += "trans_total_price_room:%.3f\n" % str2float(X["trans_total_price_room"])
            str += "list_total_price_room:%.3f\n" % str2float(X["list_total_price_room"])
            str += "trans_list_total_price_room:%.3f\n" % str2float(X["trans_list_total_price_room"])

            log.debug("%s", str)
            """
            return np.array(x)
        elif PriceModel.__is_hedonic__(self.name):
            return X
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
    X["is_sales_tax"] = "1"
    X["is_sole"] = "1"
    X["is_school_district"] = "1"
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
