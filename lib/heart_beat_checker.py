#coding:utf8
import threading
import time
import urllib2
import json

global failed_cnt
failed_cnt = 0
global success_cnt
success_cnt = 0
global test_url

server00_url='http://172.16.5.24:9008/?data={"resblock_id":"1111027382474","bizcircle_code":"611100412","bedroom_amount":"2","parlor_amount":"1","toilet_amount":"1","cookroom_amount":"1","build_size":"160","face_code":"100500000003|100500000007","build_end_year":"2000","fitment":"1","property_fee":"0","is_sales_tax":"1","is_sole":"1","is_school_district":"0","distance_metor":"600","total_floor":"30","floor":"10","balcony_amount":"0","frame_structure":"304400000001","garden_amount":"0","terrace_amount":"0","building_no":"21","unit_code":"11","room_no":"702","request_id":"111","uuid":"heart_beat","user_id":231,"channel_id":"1","city_id":1,"os_type":"3"}&request_type=0'
server01_url='http://172.16.5.25:9008/?data={"resblock_id":"1111027382474","bizcircle_code":"611100412","bedroom_amount":"2","parlor_amount":"1","toilet_amount":"1","cookroom_amount":"1","build_size":"160","face_code":"100500000003|100500000007","build_end_year":"2000","fitment":"1","property_fee":"0","is_sales_tax":"1","is_sole":"1","is_school_district":"0","distance_metor":"600","total_floor":"30","floor":"10","balcony_amount":"0","frame_structure":"304400000001","garden_amount":"0","terrace_amount":"0","building_no":"21","unit_code":"11","room_no":"702","request_id":"111","uuid":"heart_beat","user_id":231,"channel_id":"1","city_id":1,"os_type":"3"}&request_type=0'

def send_mail(receiver_addr_lst, subject, message):
    body = { 
            "version":'1.0',
            "method":"mail.sent",
            "group":"bigdata",
            "auth":"yuoizSsKggkjOc8vbMwS0OqYHvwTGGbB",
            "params": {
                "to":receiver_addr_lst,
                "subject":subject,
                "body":message
                }   
    }   
    data = json.dumps(body)
    url = 'http://sms.lianjia.com/lianjia/sms/send'
    try:
        request = urllib2.Request(url, data)
        urllib2.urlopen(request)
    except Exception, e:
        traceback.print_exc()
        raise e

def check_heart_beat(test_url_lst, host_tag_lst, check_freq):
    global failed_cnt
    global success_cnt
    for idx, test_url in enumerate(test_url_lst):
        host_tag = host_tag_lst[idx]
        try:
            resp_info = urllib2.urlopen(test_url, timeout=3)
            resp_dict = eval(resp_info.read())
            resp_code = resp_dict["rescode"]
            if resp_code == "-1":
                raise e
            else: success_cnt += 1
        except Exception, e:
            print e
            failed_cnt += 1
    if failed_cnt == 10:
        send_mail(["xuyansong@lianjia.com", "caibaiyin@lianjia.com", "huangfangsheng@lianjia.com"], "HousePricePredictionAPI", "CALLING API IS FAILED\n")
        failed_cnt = 0
    if success_cnt == 1500:
        print success_cnt
        send_mail(["xuyansong@lianjia.com", "caibaiyin@lianjia.com", "huangfangsheng@lianjia.com"], "HousePricePredictionAPI", "CALLING API IS OK, HAVE A GOOD DAY!\n")
        print "mail sent"
        success_cnt = 0

    t = threading.Timer(check_freq, check_heart_beat, (test_url_lst, host_tag_lst, check_freq))
    t.start()

if __name__ == "__main__":
    check_heart_beat([server00_url, server01_url], ["server00","server01"], 60)
