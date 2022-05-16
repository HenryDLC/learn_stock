import pandas
import pandas as pd
import akshare as ak
import datetime
import os
from DataFrameReadCsv_import_mysql import save_stockdata_mysql

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
                os_code_list_daily.append(file[:6])

        for root, dirs, files in os.walk(file_dir_weekly, topdown=False):
            for file in files:
                os_code_list_weekly.append(file[:6])

        for root, dirs, files in os.walk(file_dir_monthly, topdown=False):
            for file in files:
                os_code_list_monthly.append(file[:6])

        daily_local_stock = list(set(chinese_stock_code).difference(set(os_code_list_daily)))
        weekly_local_stock = list(set(chinese_stock_code).difference(set(os_code_list_weekly)))
        monthly_local_stock = list(set(chinese_stock_code).difference(set(os_code_list_monthly)))
        return daily_local_stock, weekly_local_stock, monthly_local_stock

    # 股票当前信息
    def stock_info(self):
        return ak.stock_zh_a_spot_em()

    # A股市场股票代码
    def stock_code_list(self):
        stock_info = self.stock_info()
        return [str(x) for x in list(stock_info['代码'])]

    # 个股历史行情
    def stock_data_info(self, start_date, end_date, symbol, header):
        try:
            if self.period in ['daily', 'weekly', 'monthly']:
                stock_data = ak.stock_zh_a_hist(symbol, self.period, start_date, end_date, self.adjust)
                if self.save == "True":
                    self.save_data_csv(symbol, stock_data, header)
                return symbol, stock_data

            elif self.period in ['1', '5', '15', '30', '60']:
                stock_data = pd.DataFrame({'one': '目前不支持分时数据'})
                if self.save == "True":
                    self.save_data_csv(symbol, stock_data)
                return symbol, stock_data
        except:
            self.error_getcode_list.append(symbol)
            print('获取失败股票代码：', self.error_writecode_list, self.period)

    # 保存数据到csv文件
    def save_data_csv(self, symbol, stock_data, header=True):
        # 增加数据
        try:
            if self.period == 'daily':
                try:
                    df = pd.read_csv(
                        "./stock_data/daily/{symbol}_{period}.csv".format(symbol=symbol, period=self.period))
                    frames = [df, stock_data]
                    result = pd.concat(frames)
                    # 查重
                    result.drop_duplicates(subset=None, keep='first', inplace=False)
                    result.to_csv("./stock_data/daily/{symbol}_{period}.csv".format(symbol=symbol, period=self.period),
                                  mode='a', header=header, index=False)
                except:
                    stock_data.to_csv(
                        "./stock_data/daily/{symbol}_{period}.csv".format(symbol=symbol, period=self.period),
                        mode='a', header=header, index=None)

            elif self.period == 'weekly':
                try:
                    df = pd.read_csv(
                        "./stock_data/weekly/{symbol}_{period}.csv".format(symbol=symbol, period=self.period))
                    frames = [df, stock_data]
                    result = pd.concat(frames)
                    # 查重
                    result.drop_duplicates(subset=None, keep='first', inplace=False)
                    result.to_csv("./stock_data/weekly/{symbol}_{period}.csv".format(symbol=symbol, period=self.period),
                                  mode='a', header=header, index=False)
                except:
                    stock_data.to_csv(
                        "./stock_data/weekly/{symbol}_{period}.csv".format(symbol=symbol, period=self.period),
                        mode='a', header=header, index=None)
            elif self.period == 'monthly':
                try:
                    df = pd.read_csv(
                        "./stock_data/monthly/{symbol}_{period}.csv".format(symbol=symbol, period=self.period))
                    frames = [df, stock_data]
                    result = pd.concat(frames)
                    # 查重
                    result.drop_duplicates(subset=None, keep='first', inplace=False)
                    result.to_csv(
                        "./stock_data/monthly/{symbol}_{period}.csv".format(symbol=symbol, period=self.period),
                        mode='a', header=header, index=False)
                except:
                    stock_data.to_csv(
                        "./stock_data/monthly/{symbol}_{period}.csv".format(symbol=symbol, period=self.period),
                        mode='a',
                        header=header, index=None)
            print(symbol, '', 'save ok')

        except:
            self.error_writecode_list.append(symbol)
            print('写入失败股票代码：', self.error_writecode_list, self.period)


# period='daily'; choice of {'daily', 'weekly', 'monthly'}
if __name__ == '__main__':
    # 4955只股票，13只股票没代码
    # 获取股票数据
    # 今日日期
    today = '20' + str(datetime.date.today().strftime('%y%m%d'))
    date = ['daily', 'weekly', 'monthly']

    for i in date:
        stock = StockData(period=i, save='True')
        # A股市场代码
        chinese_stock_code = stock.stock_code_list()
        # 本地没有的股票数据
        daily_local_stock_code, weekly_local_stock_code, monthly_local_stock_code = stock.local_stock_code(
            chinese_stock_code)

        local_stock_code = []
        if i == 'daily':
            local_stock_code = daily_local_stock_code
        elif i == 'weekly':
            local_stock_code = weekly_local_stock_code
        elif i == 'monthly':
            local_stock_code = monthly_local_stock_code
        # 获取新股票数据
        for code in local_stock_code:
            stock.stock_data_info(symbol='{code}'.format(code=code), start_date='19890101', end_date=today,
                                  header=True)

        # 更新今日数据
        for code in chinese_stock_code:
            stock.stock_data_info(symbol='{code}'.format(code=code), start_date=today, end_date=today,
                                  header=False)
        # 保存到数据库
        save_stockdata_mysql(i)
