# coding: utf8

import re
import sys
sys.path.append('conf')
import conf as conf

import math
import datetime
from calendar import month


Directions = {"东": 6, "西": 2, "南": 8, "北": 4, "东北": 5, "东南": 7, "西北": 3, "西南": 5}


def get_frame_structure(string):
    """
    get frame structure,the message is
        304400000001   平层
        304400000002   跃层
        304400000003   复式
        304400000004   错层
        304400000005   LOFT
        304400000006   跃复一体
    """
    string = string.strip()
    if string == '\N' or string.upper() == "NULL":
        return 1
    else:
        if string == "304400000001":
            return 1
        elif string == "304400000002":
            return 2
        elif string == "304400000003":
            return 3
        elif string == "304400000004":
            return 4
        elif string == "304400000005":
            return 5
        elif string == "304400000006":
            return 6
        else:
            return 1


def get_frame_structure_deploy(string):
    string = string.strip()
    if string == '\N' or string.upper() == "NULL":
        return "1\t0\t0\t0\t0\t0\t0"
    else:
        if string == "304400000001":
            return "0\t1\t0\t0\t0\t0\t0"
        elif string == "304400000002":
            return "0\t0\t1\t0\t0\t0\t0"
        elif string == "304400000003":
            return "0\t0\t0\t1\t0\t0\t0"
        elif string == "304400000004":
            return "0\t0\t0\t0\t1\t0\t0"
        elif string == "304400000005":
            return "0\t0\t0\t0\t0\t1\t0"
        elif string == "304400000006":
            return "0\t0\t0\t0\t0\t0\t1"
        else:
            return "1\t0\t0\t0\t0\t0\t0"


# 计算成交时间，距离对比的月份的距离
def get_dealdate_fromnow(string):
    basetime = "20150701"
    string = string.strip()
    
    chkedtime = datetime.datetime.strptime(string, "%Y%m%d")
    chkedyear = chkedtime.year
    chkedmonth = chkedtime.month   
    chkedday = chkedtime.day

    basetime = datetime.datetime.strptime(basetime, "%Y%m%d")
    baseyear = basetime.year
    basemonth = basetime.month
    baseday = basetime.day

    return (chkedyear - baseyear) * 12 + chkedmonth - basemonth


# done
def get_build_end_year_fromnow(string):
    string = string.strip()
    temp = int(string)-int(conf.NOW_YEAR)
    # xys_add--将时间分段
    # if temp >= 0: temp = temp/3.0 + 0.5
    # if temp < 0: temp = temp/3.0 - 0.5
    return temp


# done
def get_floor(string):
    string=string.strip()
    if(string == '地1' or string == '地下1'):
        return -1
    if(string.endswith('A') or string.endswith('B') or string.endswith('C') or string.endswith('F') or\
         string.endswith('-') or string.endswith('G') or string.endswith("E")  ):
        string = string[0:-1]
    string = string.strip()
    idx = string.find('层')
    if(idx != -1):
        string = string[0:idx]
        string = string.strip()
    if(string.startswith('A') or string.startswith('B') or string.startswith('C') or\
        string.startswith('D') or string.startswith('l') or string.startswith('S')):
        string = string[1:]
        string = string.strip()
 
    floor = int(string)
    return floor


# done
def get_total_floor(string):
    string = string.strip()
    return int(string)


# done
def get_floor_total_floor_scale(floor, total_floor):
    if total_floor <= 0:
        return -1
    if total_floor < 8:
        return math.sin(floor*math.pi/total_floor)
    else:
        if floor == 1:
            return 0.1
        elif floor == total_floor:
            return 0.5
        else:
            return floor*1.0/total_floor*1.0


# done,对于满五唯一， 还是不太了解
def get_is_sales_tax(string):
    string = string.strip()
    if string == '\N' or string.upper() == "NULL":
        return 0
    else:
        try:
            return int(string)
        except Exception, e:
            print Exception, ":", e
            return 0


# done
def get_is_sole(string):
    string = string.strip()
    if string == '\N' or string.upper() == "NULL":
        return 0
    else:
        try:
            return int(string)
        except Exception, e:
            print Exception, ":", e
            return 0


# done
def get_balcony_amount(string):
    string = string.strip()
    if len(string) == 0 or string == '' or string == 'NULL':
        return 0
    return int(string)


# done
def get_garden_amount(string):
    string = string.strip()
    if len(string) == 0 or string == '' or string == 'NULL':
        return 0
    return int(string)


def get_terrace_amount(string):
    string = string.strip()
    if len(string) == 0 or string == '' or string == 'NULL':
        return 0
    return int(string)


def get_balcony_area(string):
    string = string.strip()
    if len(string) == 0 or string == '' or string == 'NULL':
        return 0
    return float(string)    


# done
def get_distance_metro_code(string0):
    """
    get distance of metro,the message is
        20863  500400000100    500米内
        595285 500400000200    1000米内
        268042 500400000300    1500米内
        135421 500400000400    2000米内
        115715 500400000500    2500米以内
        236672 500400000600    2500米以上
        1152958 NULL    NULL
    """
    string = string0.strip()
    if string in ["0", "1", "2", "3", "4", "5", "6"]:  # 兼容离线评测数据
        return int(string)
    if len(string) < 12:
        try:
            distance = int(string)
        except Exception, e:
            print Exception, ":", e
            return 0
        if distance > 2500:
            return 1
        elif distance > 2000:
            return 2
        elif distance > 1500:
            return 3
        elif distance > 1000:
            return 4
        elif distance > 500:
            return 5
        else:
            return 6
    if string == '\N' or string.upper() == "NULL":
        return 0
    elif string == '500400000600':
        return 1
    elif string == '500400000500':
        return 2
    elif string == '500400000400':
        return 3
    elif string == '500400000300':
        return 4
    elif string == '500400000200':
        return 5
    elif string == '500400000100':
        return 6
    else:
        return 0
    return 0  # default val


# done
def get_is_school_district(string):
    string = string.strip()
    #添加 level<3 的条件
    if string == '\N' or string.upper() == 'NULL' or int(string) < 3:
        return 0
    else:
        try:
            return 1
        except Exception, e:
            print Exception, ":", e
            return 0

#130434 112100000001    毛坯
#783427 112100000002    简装
#1268850 112100000004    精装
#1042245 NULL    NULL
def get_fitment_type_code(string):
    string=string.strip()
    if( string=='\N' or string.upper()=="NULL"):
        return 3
    elif(string=='112100000001'):
        return 1
    elif(string=='112100000002'):
        return 2
    elif(string=='112100000004'):
        return 3
    else :
        return 3

#done
def get_property_fee(string):
    string=string.strip()
    if( string=='\N' or string.upper()=="NULL"):
        return 0
    else :
        try:
            return float(string)
        except Exception,e:
            return 0
#----------------
#100500000001 东
#100500000002
#100500000003 南
#100500000004
#100500000005 西
#100500000006
#100500000007 北
#100500000008 东南
#100500000009 西南
#100500000010 东北
#100500000011 西北

    
def get_face_new(string):
    string = string.strip()
    string = string.replace('100500000008','100500000010')
    string = string.replace('100500000002','100500000008')
    string = string.replace('100500000004','100500000009')
    string = string.replace('100500000006','100500000011')

    #兼容merge表数据
    string = string.replace("200300000001","100500000001")
    string = string.replace("200300000002","100500000008")
    string = string.replace("200300000003","100500000003")
    string = string.replace("200300000004","100500000009")
    string = string.replace("200300000005","100500000005")
    string = string.replace("200300000006","100500000011")
    string = string.replace("200300000007","100500000007")
    string = string.replace("200300000008","100500000010")

    score = 0
    #有北无南
    if((string.find('100500000007')!=-1 or string.find('100500000010')!=-1 or string.find('100500000011')!= -1) and \
        (string.find('100500000003')== -1 and string.find('100500000008')==-1 and string.find('100500000009')==-1)):
        score = 0.0
        return 0;
    #无北无南
    if(string.find('100500000003')==-1 and string.find('100500000007')==-1 and string.find('100500000008')==-1 and\
        string.find('100500000009')==-1 and string.find('100500000010')==-1 and string.find('100500000011')==-1):
        score = 1.0
        return score
    #有南无北
    if((string.find('100500000003')!=-1 or string.find('100500000008')!=-1 or string.find('100500000009')!=-1) and\
        (string.find('100500000007')==-1 and string.find('100500000010')==-1 and string.find('100500000011')==-1)):
        score = 2.0
        return score
    #有南有北
    if((string.find('100500000003')!=-1 or string.find('100500000008')!=-1 or string.find('100500000009')!=-1) and\
        (string.find('100500000007')!=-1 or string.find('100500000010')!=-1 or string.find('100500000011')!=-1)):
        score = 3.0
        return score
    return -1.0;
    
def get_face_code(string):
    string=string.strip()
    if( string=='\N' or string.upper()=="NULL"):
        return 0
    else:
        return string
 
def get_face_name(string):
    string=string.strip()
    if( string=='\N' or string.upper()=="NULL"):
        return 0
    ret=0
    splits=re.split(u';|,', string)
    scale=0.5
    for i in range(len(splits)):
        if(Directions.has_key(splits[i].strip()) ):
           ret+=Directions.get(splits[i].strip())*(math.pow(0.5, i))     
    return ret
  
#######################################################
#1 101300000002    商品房（市属）
#1911170 102200000001    板楼
# 859422 102200000002    塔楼
# 359929 102200000003    板塔结合
#  15646 102200000004    平房
#   3173 102200000005    筒子楼
#   1318 102200000006    简易楼
#   3800 102200000007    产权车库
#   2983 102200000008    四合院
#  15005 102200000009    别墅
#  51625 102200000010    其他
#      2 107500000003    普通住宅
#    882 NULL    NULL       
def get_build_type_code(string):
    string=string.strip()
    if( string=='\N' or string.upper()=="NULL"):
        return 10
    elif (string=="102200000001"):
        return 1
    elif (string=="102200000002"):
        return 2
    elif (string=="102200000003"):
        return 3
    elif (string=="102200000004"):
        return 4
    elif (string=="102200000005"):
        return 5
    elif (string=="102200000006"):
        return 6
    elif (string=="102200000007"):
        return 7
    elif (string=="102200000008"):
        return 8
    elif (string=="102200000009"):
        return 9
    elif (string=="102200000010"):
        return 10
    else:
        return -1


def get_house_property_code(string):
    string=string.strip()
    if( string=='\N' or string.upper()=="NULL"):
        return 0
    elif(string=="101300000001"):
        return 1
    elif(string=="101300000002"):
        return 2
    elif(string=="101300000003"):
        return 3
    elif(string=="101300000004"):
        return 4
    elif(string=="101300000005"):
        return 5
    elif(string=="101300000006"):
        return 6
    elif(string=="101300000007"):
        return 7
    elif(string=="101300000008"):
        return 8
    elif(string=="101300000009" or string=="101302200008"):
        return 9
    elif(string=="1013000000010" or string=="101302200001" or string=="905600000001"):
        return 10
    elif(string=="101300000011"):
        return 11
    elif(string=="101300000012"):
        return 12
    elif(string=="101300000013" or string=="101302200009" or string=="905600000006"):
        return 13
    elif(string=="101300000014"):
        return 14
    elif(string=="101300000015"):
        return 15
    elif(string=="101300000016"):
        return 16
    elif(string=="101302200002"):
        return 17
    elif(string=="101302200003"):
        return 18
    elif(string=="905600000002"):
        return 19
    else :
        return 13

    
def get_house_usage_code(string):
    string=string.strip()
    if( string=='\N' or string.upper()=="NULL"):
        return 0
    elif(string=="107500000001"):
        return 1
    elif(string=="107500000002"):
        return 2
    elif(string=="107500000003"):
        return 3
    elif(string=="107500000004"):
        return 4
    elif(string=="107500000005"):
        return 5
    elif(string=="107500000006"):
        return 6
    elif(string=="107500000007"):
        return 7
    elif(string=="107500000008"):
        return 8
    elif(string=="107500000009"):
        return 9
    elif(string=="107500000010"):
        return 10
    elif(string=="107500000011"):
        return 11
    elif(string=="107500000012"):
        return 12
    elif(string=="107500000012"):
        return 13
    else:
        return 0
              

            
if __name__ =="__main__":
   print get_floor_total_floor_scale(7,7);
   
  
    
    
    
    
    
    
    
     

  
