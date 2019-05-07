# -*- coding: utf-8 -*-
import scrapy

from urllib.parse import urljoin


class StatesSpider(scrapy.Spider):
    name = 'states'
    allowed_domains = ['stats.gov.cn']
    start_urls = ['http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/index.html']
    base_url = 'http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/index.html'

    def parse(self, response):
        # 获取省级元素
        province_list = response.xpath('//tr[@class="provincetr"]/td')
        for td in province_list:
            province_name = td.xpath('./a/text()').get()
            province_url = td.xpath('./a/@href').get()

            if not province_url:
                return

            code = province_url.split('.')[0]
            province_url = urljoin(self.base_url, province_url)

            yield scrapy.Request(
                url=province_url,
                callback=self.parse_city,
                meta={'name': province_name, 'code': code}
            )

            # 返回当前城市信息
            data = {'area_code': code,
                    'level': 'province',
                    'area_name': province_name,
                    'full_name': province_name,
                    'parent': 0}
            yield data


    def parse_city(self, response):
        """
        获取市级信息
        :param response:
        :return:
        """
        {'area_code': '330106109002',
         'level': 'village',
         'area_name': '新星社区',
         'full_name': '浙江省杭州市西湖区三墩镇新星社区',
         'parent': '33010610900'}

        parent_code = response.meta.get('code')
        parent_name = response.meta.get('name')

        city_list = response.xpath('//tr[@class="citytr"]')
        for city in city_list:
            city_code = city.xpath('./td[1]//text()').get()
            city_url = city.xpath('./td[1]/a/@href').get()
            city_name = city.xpath('./td[2]//text()').get()

            # 返回当前城市信息
            data = {'area_code': city_code,
                    'level': 'city',
                    'area_name': city_name,
                    'full_name': parent_name + city_name,
                    'parent': parent_code}
            yield data

            if city_url:
                city_url = urljoin(self.base_url, city_url)

                # 请求下一级区域
                yield scrapy.Request(
                    url=city_url,
                    callback=self.parse_area,
                    meta=data
                )

    def parse_area(self, response):
        """
        获取县区级信息
        :param response:
        :return:
        """
        parent_code = response.meta.get('area_code')
        parent_name = response.meta.get('full_name')

        prefix_list = response.url.split('/')[:-1]
        prefix_url = '/'.join(prefix_list)

        area_list = response.xpath('//tr[@class="countytr"]')
        for area in area_list:
            area_code = area.xpath('./td[1]//text()').get()
            area_url = area.xpath('./td[1]/a/@href').get()
            area_name = area.xpath('./td[2]//text()').get()

            # 返回当前城市信息
            data = {'area_code': area_code,
                    'level': 'area',
                    'area_name': area_name,
                    'full_name': parent_name + area_name,
                    'parent': parent_code}
            yield data

            if area_url:
                area_url = prefix_url + '/' + area_url
                # 请求下一级区域
                yield scrapy.Request(
                    url=area_url,
                    callback=self.parse_town,
                    meta=data
                )

    def parse_town(self, response):
        """
        获取县区级信息
        :param response:
        :return:
        """
        parent_code = response.meta.get('area_code')
        parent_name = response.meta.get('full_name')

        prefix_list = response.url.split('/')[:-1]
        prefix_url = '/'.join(prefix_list)

        town_list = response.xpath('//tr[@class="towntr"]')
        for town in town_list:
            town_code = town.xpath('./td[1]//text()').get()
            town_url = town.xpath('./td[1]/a/@href').get()
            town_name = town.xpath('./td[2]//text()').get()

            # 返回当前城市信息
            data = {'area_code': town_code,
                    'level': 'town',
                    'area_name': town_name,
                    'full_name': parent_name + town_name,
                    'parent': parent_code}
            yield data

            if town_url:
                town_url = prefix_url + '/' + town_url

                # 请求下一级区域
                yield scrapy.Request(
                    url=town_url,
                    callback=self.parse_village,
                    meta=data
                )

    def parse_village(self, response):
        """
        获取县区级信息
        :param response:
        :return:
        """
        parent_code = response.meta.get('area_code')
        parent_name = response.meta.get('full_name')

        village_list = response.xpath('//tr[@class="villagetr"]')
        for village in village_list:
            village_code = village.xpath('./td[1]/text()').get()
            village_name = village.xpath('./td[3]/text()').get()

            # 返回当前城市信息
            data = {'area_code': village_code,
                    'level': 'village',
                    'area_name': village_name,
                    'full_name': parent_name + village_name,
                    'parent': parent_code}
            yield data

