import pandas as pd
from sqlalchemy import create_engine
import os
import datetime


class save_stockdata_mysql(object):
    def __init__(self, date):
        self.date = date
        self.error_code = []

        # stock_daily_conn = create_engine('mysql+pymysql://root:123456@82.156.26.93:3306/stock_daily?charset=utf8mb4')
        # stock_weekly_conn = create_engine('mysql+pymysql://root:123456@82.156.26.93:3306/stock_weekly?charset=utf8mb4')
        # stock_monthly_conn = create_engine(
        #     'mysql+pymysql://root:123456@82.156.26.93:3306/stock_monthly?charset=utf8mb4')

        stock_daily_conn = create_engine('mysql+pymysql://root:707116148@localhost:3306/stock_daily?charset=utf8mb4')
        stock_weekly_conn = create_engine('mysql+pymysql://root:707116148@localhost:3306/stock_weekly?charset=utf8mb4')
        stock_monthly_conn = create_engine(
            'mysql+pymysql://root:707116148@localhost:3306/stock_monthly?charset=utf8mb4')

        file_dir_daily = './stock_data/daily'
        file_dir_weekly = './stock_data/weekly'
        file_dir_monthly = './stock_data/monthly'

        os_code_list_daily = []
        os_code_list_weekly = []
        os_code_list_monthly = []
        for root, dirs, files in os.walk(file_dir_daily, topdown=False):
            for file in files:
                if file != '.DS_Store' and file != '.gitkeep':
                    os_code_list_daily.append(file[:12])

        for root, dirs, files in os.walk(file_dir_weekly, topdown=False):
            for file in files:
                if file != '.DS_Store' and file != '.gitkeep':
                    os_code_list_weekly.append(file[:13])

        for root, dirs, files in os.walk(file_dir_monthly, topdown=False):
            for file in files:
                if file != '.DS_Store' and file != '.gitkeep':
                    os_code_list_monthly.append(file[:14])

        if self.date == 'daily':
            self.file_dir = file_dir_daily
            self.engine = stock_daily_conn
            self.os_code_list = os_code_list_daily
            self.file_dir = file_dir_daily
        elif self.date == 'weekly':
            self.file_dir = file_dir_weekly
            self.engine = stock_weekly_conn
            self.os_code_list = os_code_list_weekly
            self.file_dir = file_dir_weekly
        elif self.date == 'monthly':
            self.file_dir = file_dir_monthly
            self.engine = stock_monthly_conn
            self.os_code_list = os_code_list_monthly
            self.file_dir = file_dir_monthly
        # 今日日期
        self.today = '20' + str(datetime.date.today().strftime('%y%m%d'))

    def run(self):
        for i in self.os_code_list:
            print("处理开始：", str(i))
            try:
                df_csv = pd.read_csv('{file_dir}/{code}.csv'.format(file_dir=self.file_dir, code=i), index_col='日期',
                                     parse_dates=True,
                                     na_values=['nan', 'Nan', 'NaN', 'NaT', '', ''])

                try:
                    df_sql = pd.read_sql('select * from {table_name}'.format(table_name=i), con=self.engine)
                    df_sql_data = df_sql.tail(1)
                    df_sql_data = pd.to_datetime(df_sql_data.index, unit='D')

                    # 更新起止时间
                    update_date = str(pd.to_datetime(df_sql_data) - datetime.timedelta(days=1))[16:26].replace("-", "")
                    if df_sql_data.size == 0:
                        raise Exception("df_sql_date_zero")
                    if bool(self.today > df_sql_data):
                        df_csv[update_date::].to_sql(str(i), con=self.engine, if_exists='append')

                except Exception as e:
                    print('存储', e)
                    df_csv.to_sql(str(i), con=self.engine, if_exists='replace')

                print('Write to MySQL successfully: ', i)
            except Exception as e:
                print('读取csv：', e)
                self.error_code.append(i)

        print('处理error' + str(self.error_code))
