import pandas as pd
import akshare as ak
import datetime

'''
# 临时代码
    # 找出还没写入的股票数据
    import os
    file_dir = './stock_data/daily'
    os_code_list = []
    for root, dirs, files in os.walk(file_dir, topdown=False):
        for file in files:
            os_code_list.append(file[:6])

    stock = StockData(period='daily', save='True')
    stock_code = stock.stock_code_list()
    # 获取非交集代码
    # stock_code A股所有股票代码
    # os_code_list 本地已经拥有的股票数据代码
    stock_code = list(set(stock_code).difference(set(os_code_list)))
    
    
    # 测试 日、周、月数据完整性
    import os

    file_dir_daily = './stock_data/daily'
    file_dir_weekly = './stock_data/weekly'
    file_dir_monthly = './stock_data/monthly'
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
            
    stock_code = list(set(os_code_list_daily).difference(set(os_code_list_monthly)))
    print(stock_code)
'''


class StockData(object):
    def __init__(self, period='daily', save='False'):
        self.period = period
        self.start_date = "19900101"
        self.end_date = '20' + str(datetime.date.today().strftime('%y%m%d'))  # 当天日期
        self.adjust = ""
        self.save = save
        self.error_getcode_list = []
        self.error_writecode_list = []

    # 股票当前信息
    def stock_info(self):
        return ak.stock_zh_a_spot_em()

    # A股市场股票代码
    def stock_code_list(self):
        stock_info = self.stock_info()
        return [str(x) for x in list(stock_info['代码'])]

    # 个股历史行情
    def stock_data_info(self, symbol):
        try:
            if self.period in ['daily', 'weekly', 'monthly']:
                stock_data = ak.stock_zh_a_hist(symbol, self.period, self.start_date,
                                                self.end_date, self.adjust)
                if self.save == "True":
                    self.save_data_csv(symbol, stock_data)
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
    def save_data_csv(self, symbol, stock_data):
        try:
            if self.period == 'daily':
                stock_data.to_csv("./stock_data/daily/{symbol}_{period}.csv".format(symbol=symbol, period=self.period))
            elif self.period == 'weekly':
                stock_data.to_csv("./stock_data/weekly/{symbol}_{period}.csv".format(symbol=symbol, period=self.period))
            elif self.period == 'monthly':
                stock_data.to_csv(
                    "./stock_data/monthly/{symbol}_{period}.csv".format(symbol=symbol, period=self.period))
            print(symbol, '', 'save ok')

        except:
            self.error_writecode_list.append(symbol)
            print('写入失败股票代码：', self.error_writecode_list, self.period)


# period='daily'; choice of {'daily', 'weekly', 'monthly'}
if __name__ == '__main__':
    # 4955只股票，13只股票没代码
    # 获取股票数据
    stock = StockData(period='monthly', save='True')
    stock_code = stock.stock_code_list()
    for i in stock_code:
        stock.stock_data_info(symbol='{code}'.format(code=i))

