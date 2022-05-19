import pandas as pd
from sqlalchemy import create_engine
import os

stock_daily_conn = create_engine('mysql+pymysql://root:123456@82.156.26.93:3306/stock_daily?charset=utf8mb4')
stock_weekly_conn = create_engine('mysql+pymysql://root:123456@82.156.26.93:3306/stock_weekly?charset=utf8mb4')
stock_monthly_conn = create_engine('mysql+pymysql://root:123456@82.156.26.93:3306/stock_monthly?charset=utf8mb4')

file_dir_daily = './stock_data/daily'
file_dir_weekly = './stock_data/weekly'
file_dir_monthly = './stock_data/monthly'
os_code_list_daily = []
os_code_list_weekly = []
os_code_list_monthly = []
for root, dirs, files in os.walk(file_dir_daily, topdown=False):
    for file in files:
        os_code_list_daily.append(file[:12])

for root, dirs, files in os.walk(file_dir_weekly, topdown=False):
    for file in files:
        os_code_list_weekly.append(file[:13])

for root, dirs, files in os.walk(file_dir_monthly, topdown=False):
    for file in files:
        os_code_list_monthly.append(file[:14])


class save_stockdata_mysql(object):
    def __init__(self, date):
        self.date = date
        self.error_code = []

        if self.date == 'daily':
            self.file_dir = file_dir_daily
            self.engine = stock_daily_conn
            self.os_code_list = os_code_list_daily
        elif self.date == 'weekly':
            self.file_dir = file_dir_weekly
            self.engine = stock_weekly_conn
            self.os_code_list = os_code_list_weekly
        elif self.date == 'monthly':
            self.file_dir = file_dir_monthly
            self.engine = stock_weekly_conn
            self.os_code_list = os_code_list_monthly

    def run(self):
        for i in self.os_code_list:
            try:
                print(i)
                df = pd.read_csv('{file_dir}/{code}.csv'.format(file_dir=self.file_dir, code=i), index_col='日期',
                                 parse_dates=True,
                                 na_values=['nan', 'Nan', 'NaN', 'NaT', '', ''])

                df = df[['开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']]
                df.to_sql(str(i), con=self.engine, if_exists='append')
                print('Write to MySQL successfully: ', i)
            except:
                self.error_code.append(i)

        print(self.error_code)


# test = save_stockdata_mysql('daily')
# test.run()
