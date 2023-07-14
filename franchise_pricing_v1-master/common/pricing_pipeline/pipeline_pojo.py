import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, '..'))
from common.pricing_pipeline.pipeline import SEVEN_CHANNELS_MAP

'''
    {
    "writeTimestamp": 14598876999, 
    "prices": [
        {
            "date": "2019-03-05", 
            "oyoId": "CN_CHA010", 
            "hotelName": "83295 OYO酒店（常州钟楼南大街店）", 
            "roomTypeId": 20, 
            "roomTypeName": "标准大床房", 
            "finalPrice": 89, 
            "hourlyPrice": 53, 
            "changePriceOtas": [
                {
                    "otaChannelId": 1, 
                    "otaChannelName": "携程", 
                    "preCommissionRate": 10, 
                    "postCommissionRate": 15
                }, 
                {
                    "otaChannelId": 4, 
                    "otaChannelName": "艺龙", 
                    "preCommissionRate": 10, 
                    "postCommissionRate": 10
                }
            ]
         }
      ]
    }
'''
from common.util.utils import JsonUtil


class JSONSerializable:
    def to_json(self):
        return JsonUtil.json_serialize(self)


class OtaPricing(JSONSerializable):
    def __init__(self, prices, write_timestamp):
        self.prices = prices
        self.writeTimestamp = write_timestamp

    def to_json(self):
        return JsonUtil.json_serialize(self)


class Prices(JSONSerializable):
    def __init__(self, date, oyo_id, hotel_name, room_type_id, room_type_name, change_price_otas):
        self.date = date
        self.oyoId = oyo_id
        self.hotelName = hotel_name
        self.roomTypeId = room_type_id
        self.roomTypeName = room_type_name
        self.changePriceOtas = change_price_otas


class ChangePriceOta(JSONSerializable):
    def __init__(self, ota_channel_name, pre_commission_rate, post_commission_rate, pre_sell_price, post_sell_price,
                 hourly_price, pre_breakfast_price, post_breakfast_price):
        self.otaChannelId = SEVEN_CHANNELS_MAP[ota_channel_name]
        self.otaChannelName = ota_channel_name
        self.preCommissionRate = pre_commission_rate
        self.postCommissionRate = post_commission_rate
        self.preSellPrice = pre_sell_price
        self.postSellPrice = post_sell_price
        self.hourlyPrice = hourly_price
        self.preBreakfastPrice = pre_breakfast_price
        self.postBreakfastPrice = post_breakfast_price


class PmsPrice(JSONSerializable):
    def __init__(self, hotelId, date, roomTypeId, rate, operateId):
        self.hotelId = hotelId
        self.date = date
        self.roomTypeId = roomTypeId
        self.rate = rate
        self.operateId = operateId


class PmsBatchPrice(JSONSerializable):
    def __init__(self, dataList):
        self.dataList = dataList

    def to_json(self):
        return JsonUtil.json_serialize(self)


class LmsPrice(JSONSerializable):
    def __init__(self, hotelId, roomTypeId, channelId, rateDate, calculateType, hourlyRate, changeType, remark):
        self.hotelId = hotelId
        self.roomTypeId = roomTypeId
        self.channelId = channelId
        self.rateDate = rateDate
        self.calculateType = calculateType
        self.hourlyRate = hourlyRate
        self.changeType = changeType
        self.remark = remark


if __name__ == '__main__':
    changePriceOtaA = ChangePriceOta("ctrip", 10, None)
    changePriceOtaA = JsonUtil.json_serialize(changePriceOtaA)
    changePriceOtaB = ChangePriceOta("elong", 10, 10)
    changePriceOtas = [changePriceOtaA, changePriceOtaB]
    priceA = Prices("2019-03-05", "CN_CHA010", "83295 OYO酒店（常州钟楼南大街店）", "20", "标准大床房", 89, 53, changePriceOtas)
    priceB = Prices("2018-03-01", "CN_CHA015", "83195 OYO酒店（望京南大街店）", "21", "标准大床房", 79, 23, changePriceOtas)
    pricesArr = [priceA, priceB]
    otaPricing = OtaPricing(121213323, pricesArr)
    print(otaPricing.to_json(), file=open("./errorParamsJson{0}.json".format(1), 'w+'))
    print(otaPricing.to_json())
