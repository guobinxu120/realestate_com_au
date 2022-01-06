# -*- coding: utf-8 -*-

import scrapy
from scrapy import Request
from scrapy.selector import Selector
from json import loads
import csv, os

class CategoriesOfComperdeliveryComBr(scrapy.Spider):

    name = "realestate"
    start_urls = ('https://www.comperdelivery.com.br/',)

    use_selenium = False
    result_path = 'result/'
    # ////////////////////////////median csv///////////////////////////////////////////////////////
    file_name1 = result_path + "Median.csv"
    f1 = open(file_name1, "w", newline='')
    writer_median = csv.writer(f1, delimiter=',',quoting=csv.QUOTE_ALL)
    is_write_header_median = False
    # //////////////////////////market csv/////////////////////////////////////////////////////////
    file_name1 = result_path + "Market.csv"
    f2 = open(file_name1, "w", newline='')
    writer_market = csv.writer(f2, delimiter=',',quoting=csv.QUOTE_ALL)
    is_write_header_market = False

    def start_requests(self):
        f1 = open('locations.csv')
        csv_items = csv.DictReader(f1)
        self.locations =[]
        for i, row in enumerate(csv_items):
            self.locations.append(row)
        f1.close()

        for location in self.locations:
            city = location['city'].strip().replace(' ', '%20')
            code = location['code'].strip().replace(' ', '%20')
            state = location['state'].strip()
            yield Request('https://investor-api.realestate.com.au/states/{}/suburbs/{}/postcodes/{}/sold_trend_data.json'.format(state, city, code),
                          callback=self.parse_Median, meta={'city':city, 'code':code, 'state':state})
            yield Request('https://investor-api.realestate.com.au/states/{}/suburbs/{}/postcodes/{}/rental_supply_demand_ratio_trend_data.json'.format(state, city, code),
                          callback=self.parse_Market, meta={'city':city, 'code':code, 'state':state})


    def parse_Median(self, response):
        city = response.meta['city'].replace('%20', ' ')
        code = response.meta['code']
        state = response.meta['state']
        try:
            json_data = loads(response.body)
            # result_path = "result/{}_{}/".format(city, code)
            # if not os.path.exists(result_path):
            #     os.makedirs(result_path)
            data_keys = json_data['property_type'].keys()

            # is_write_header = False
            for file_name in data_keys:
                # unit_file_name = result_path + "{}_{}_Median.csv".format(city, file_name)
                years = json_data['property_type'][file_name]['bedrooms']['ALL']['yearly'].keys()
                years = sorted(years)
                years.insert(0, 'bedrooms')
                headers = ['state', 'suburb', 'postcode']
                for key in years:
                    headers.append(key)
                if not self.is_write_header_median:
                    self.writer_median.writerow(headers)
                    self.is_write_header_median = True
                for key in json_data['property_type'][file_name]['bedrooms'].keys():
                    if key == '0': continue
                    datas = []
                    bedroom_data = json_data['property_type'][file_name]['bedrooms'][key]
                    for year in years:
                        if year == 'bedrooms': continue
                        if 'yearly' in bedroom_data.keys() and year in bedroom_data['yearly'].keys(): ####12_monthly
                            data = bedroom_data['yearly'][year]['value']
                            datas.append(data)
                        else:
                            datas.append('')
                    datas.insert(0, key+"_{}".format(file_name))
                    data_row = [state, city, code]
                    for data in datas:
                        data_row.append(data)

                    self.writer_median.writerow(data_row)
            # f1.close()
            # yield Request('https://investor-api.realestate.com.au/states/{}/suburbs/{}/postcodes/{}/rental_supply_demand_ratio_trend_data.json'.format(state, city, code),
            #               callback=self.parse_Market, meta={'city':city, 'code':code, 'state':state})
        except Exception as e:
            print(e)
            pass

    def parse_Market(self, response):
        city = response.meta['city'].replace('%20', ' ')
        code = response.meta['code'].strip()
        state = response.meta['state']
        try:
            json_data = loads(response.body)
            # result_path = "result/{}_{}/".format(city, code)
            # if not os.path.exists(result_path):
            #     os.makedirs(result_path)
            data_keys = json_data['property_type'].keys()

            # file_name1 = result_path + "{}_{}_Market.csv".format(city, state)
            # f1 = open(file_name1, "w", newline='')
            # writer = csv.writer(f1, delimiter=',',quoting=csv.QUOTE_ALL)
            for file_name in data_keys:
                years = json_data['property_type'][file_name]['bedrooms']['ALL']['monthly'].keys()
                years = sorted(years)
                years.insert(0, 'bedrooms')
                headers = ['state', 'suburb', 'postcode']
                for key in years:
                    headers.append(key)
                # headers = headers.extend(years)
                if not self.is_write_header_market:
                    self.writer_market.writerow(headers)
                    self.is_write_header_market = True
                for key in json_data['property_type'][file_name]['bedrooms'].keys():
                    datas = []
                    bedroom_data = json_data['property_type'][file_name]['bedrooms'][key]
                    for year in years:
                        if year == 'bedrooms': continue
                        if 'monthly' in bedroom_data.keys() and year in bedroom_data['monthly'].keys():
                            data = bedroom_data['monthly'][year]['value']
                            datas.append(data)
                        else:
                            datas.append('')
                    datas.insert(0, key + "_{}".format(file_name))
                    data_row = [state, city, code]
                    for data in datas:
                        data_row.append(data)
                    self.writer_market.writerow(data_row)
            # f1.close()
            # yield
        except Exception as e:
            print(e)
            pass
