import random

import pandas as pd
import akshare as ak
import datetime
import os
from DataFrameReadCsv_import_mysql import save_stockdata_mysql
from time import sleep

# 今日日期
today = '20' + str(datetime.date.today().strftime('%y%m%d'))
# 测试变量 当今时间
# today = '20200930'
file_dir_daily = './stock_data/daily'
file_dir_weekly = './stock_data/weekly'
file_dir_monthly = './stock_data/monthly'


class StockData(object):
    def __init__(self, period='daily', save='False'):
        self.period = period
        self.adjust = ""
        self.save = save
        self.error_getcode_list = []
        self.error_writecode_list = []

    # 本地没有的股票数据
    def local_stock_code(self, chinese_stock_code):
        # 测试 日、周、月数据完整性
        os_code_list_daily = []
        os_code_list_weekly = []
        os_code_list_monthly = []
        for root, dirs, files in os.walk(file_dir_daily, topdown=False):
            for file in files:
                if file != '.DS_Store' and file != '.gitkeep':
                    os_code_list_daily.append(file[:6])

        for root, dirs, files in os.walk(file_dir_weekly, topdown=False):
            for file in files:
                if file != '.DS_Store' and file != '.gitkeep':
                    os_code_list_weekly.append(file[:6])

        for root, dirs, files in os.walk(file_dir_monthly, topdown=False):
            for file in files:
                if file != '.DS_Store' and file != '.gitkeep':
                    os_code_list_monthly.append(file[:6])

        daily_local_stock = list(set(chinese_stock_code).difference(set(os_code_list_daily)))
        weekly_local_stock = list(set(chinese_stock_code).difference(set(os_code_list_weekly)))
        monthly_local_stock = list(set(chinese_stock_code).difference(set(os_code_list_monthly)))
        return daily_local_stock, weekly_local_stock, monthly_local_stock, os_code_list_daily, os_code_list_weekly, os_code_list_monthly

    # 股票当前信息
    def stock_info(self):
        return ak.stock_zh_a_spot_em()

    # A股市场股票代码
    def stock_code_list(self):
        stock_info = self.stock_info()
        return [str(x) for x in list(stock_info['代码'])]

    # 个股历史行情
    def stock_data_info(self, start_date, end_date, symbol, header, role=0):
        try:
            if self.period in ['daily', 'weekly', 'monthly']:
                stock_data = ak.stock_zh_a_hist(symbol, self.period, start_date, end_date, self.adjust)
                if self.save == "True" and role == 0:
                    self.save_data_csv(symbol, stock_data, header)
                return symbol, stock_data

        except Exception as e:
            print('-' * 100)
            self.error_getcode_list.append(symbol)
            print(symbol)
            print("股票数据获取失败")
            print(e)
            print('-' * 100)

    # 保存数据到csv文件
    def save_data_csv(self, symbol, stock_data, header=True):
        # 增加数据
        try:
            if self.period == 'daily':
                try:
                    df = pd.read_csv(
                        file_dir_daily + '/{symbol}_{period}.csv'.format(symbol=symbol, period=self.period),
                        index_col='日期', parse_dates=True,
                        na_values=['nan', 'Nan', 'NaN', 'NaT', '', '']).tail(1)
                    # 更新起止时间
                    update_date = str(pd.to_datetime(df.index) + datetime.timedelta(days=1))[16:26].replace("-", "")
                    if df.index.size == 0:
                        raise Exception("df_sql_date_zero")
                    if today > df.index:
                        _, stock_data = self.stock_data_info(update_date, today, symbol, header=True, role=1)
                        stock_data['日期'] = pd.to_datetime(stock_data['日期'])
                        stock_data.set_index(['日期'], inplace=True)
                        stock_data.to_csv(
                            file_dir_daily + '/{symbol}_{period}.csv'.format(symbol=symbol, period=self.period),
                            mode='a', header=False, index=True)
                except Exception as e:
                    stock_data.to_csv(
                        file_dir_daily + '/{symbol}_{period}.csv'.format(symbol=symbol, period=self.period),
                        mode='a', header=True, index=False)
            elif self.period == 'weekly':
                try:
                    df = pd.read_csv(
                        file_dir_weekly + '/{symbol}_{period}.csv'.format(symbol=symbol, period=self.period),
                        index_col='日期', parse_dates=True,
                        na_values=['nan', 'Nan', 'NaN', 'NaT', '', '']).tail(1)
                    # 更新起止时间
                    update_date = str(pd.to_datetime(df.index) + datetime.timedelta(days=1))[16:26].replace("-", "")
                    if df.index.size == 0:
                        raise Exception("df_sql_date_zero")
                    if today > df.index:
                        _, stock_data = self.stock_data_info(update_date, today, symbol, header=True, role=1)
                        stock_data['日期'] = pd.to_datetime(stock_data['日期'])
                        stock_data.set_index(['日期'], inplace=True)
                        stock_data.to_csv(
                            file_dir_weekly + '/{symbol}_{period}.csv'.format(symbol=symbol, period=self.period),
                            mode='a', header=False, index=True)
                except Exception as e:
                    stock_data.to_csv(
                        file_dir_weekly + '/{symbol}_{period}.csv'.format(symbol=symbol, period=self.period),
                        mode='a', header=True, index=False)

            elif self.period == 'monthly':
                try:
                    df = pd.read_csv(
                        file_dir_monthly + '/{symbol}_{period}.csv'.format(symbol=symbol, period=self.period),
                        index_col='日期', parse_dates=True,
                        na_values=['nan', 'Nan', 'NaN', 'NaT', '', '']).tail(1)
                    # 更新起止时间
                    update_date = str(pd.to_datetime(df.index) + datetime.timedelta(days=1))[16:26].replace("-", "")
                    if df.index.size == 0:
                        raise Exception("df_sql_date_zero")
                    if today > df.index:
                        _, stock_data = self.stock_data_info(update_date, today, symbol, header=True, role=1)
                        stock_data['日期'] = pd.to_datetime(stock_data['日期'])
                        stock_data.set_index(['日期'], inplace=True)
                        stock_data.to_csv(
                            file_dir_monthly + '/{symbol}_{period}.csv'.format(symbol=symbol, period=self.period),
                            mode='a', header=False, index=True)
                except Exception as e:
                    stock_data.to_csv(
                        file_dir_monthly + '/{symbol}_{period}.csv'.format(symbol=symbol, period=self.period),
                        mode='a', header=True, index=False)
            print(symbol, '', 'save ok')

        except Exception as e:
            self.error_getcode_list.append(symbol)
            print('*' * 100)
            print(e)
            print("写入股票数据失败")
            self.error_writecode_list.append(symbol)
            print('*' * 100)

    def error_stock_code(self):
        if len(self.error_getcode_list) > 0:
            error_get_stock_code = pd.DataFrame({"save_error_stock_code": self.error_getcode_list})
            error_get_stock_code.to_csv("./log/error_get_stock_code_{period}.csv".format(period=self.period))
        if len(self.error_writecode_list) > 0:
            error_write_stock_code = pd.DataFrame({"error_write_stock_code": self.error_writecode_list})
            error_write_stock_code.to_csv("./log/error_write_stock_code_{period}.csv".format(period=self.period))


# period='daily'; choice of {'daily', 'weekly', 'monthly'}
if __name__ == '__main__':
    # 4955只股票，13只股票没代码
    # 获取股票数据

    date = ['daily', 'weekly', 'monthly']

    for i in date:
        stock = StockData(period=i, save='True')
        # A股市场代码
        chinese_stock_code = stock.stock_code_list()


        # 临时测试变量
        # A股市场代码
        # update_chinese_stock_code = []
        # # # A股市场代码+更新代码
        # chinese_stock_code = ['603717', '300931', '600039', '002072', '000502', '300737', '601116', '002985',
        #                       '002111', '600797', '002922', '000710', '600797','600008', '300537']
        # # 不可获得股票代码
        # error_stock_code = ['688120', '688287']

        # 获取新股票数据
        def get_new_stock():
            # 本地没有的股票数据
            daily_local_stock_code, weekly_local_stock_code, monthly_local_stock_code, _, _, _ = stock.local_stock_code(
                chinese_stock_code)

            no_local_stock_code = []
            if i == 'daily':
                no_local_stock_code = daily_local_stock_code
            elif i == 'weekly':
                no_local_stock_code = weekly_local_stock_code
            elif i == 'monthly':
                no_local_stock_code = monthly_local_stock_code
            print("no_local_stock_code：", no_local_stock_code)

            for code in no_local_stock_code:
                stock.stock_data_info(symbol='{code}'.format(code=code), start_date='19890101', end_date=today,
                                      header=True)


        # 获取更新数据
        def get_update_stock():
            _, _, _, os_code_list_daily, os_code_list_weekly, os_code_list_monthly = stock.local_stock_code(
                chinese_stock_code)

            local_stock_code = []
            if i == 'daily':
                local_stock_code = os_code_list_daily
            elif i == 'weekly':
                local_stock_code = os_code_list_weekly
            elif i == 'monthly':
                local_stock_code = os_code_list_monthly

            # 本地的代码和A股市场代码相同，说明股票数据表是全的，更新数据表里的数据就好，否则需要建立数据表
            if set(local_stock_code) == set(chinese_stock_code):
                for code in chinese_stock_code:
                    stock.stock_data_info(symbol='{code}'.format(code=code), start_date=today, end_date=today,
                                          header=True)
            else:
                get_new_stock()

        # TODO 执行顺序没有问题 二次更新 不是增量更新 也不是增量更新 而是 新建更新
        # print("*" * 1000)
        # print('运行在：：', i)
        # get_new_stock()
        # get_update_stock()
        # stock.error_stock_code()
        # print(i, '：运行结束')
        # print("*" * 1000)


        # TODO 存在重复添加问题，需要查重处理
        # 保存到数据库
        print("*" * 1000)
        print('运行在：：', i)
        save_stockdata_mysql(i).run()
        print(i, '：运行结束')
        print("*" * 1000)
        del stock

