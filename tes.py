import datetime

import pandas as pd
from matplotlib.dates import date2num
import akshare as ak

import pymysql

today = '20' + str(datetime.date.today().strftime('%y%m%d'))
# try:
#     df = pd.read_csv('./stock_data/daily/688327_daily.csv', index_col='日期', parse_dates=True,
#                      na_values=['nan', 'Nan', 'NaN', 'NaT', '', '']).tail(1)
#     # 更新起止时间
#     update_date = str(pd.to_datetime(df.index) + datetime.timedelta(days=1))[16:27].replace("-", "")

#     if today > df.index:
#         stock_data = ak.stock_zh_a_hist('688327', 'daily', update_date, today, adjust='')
#         stock_data['日期'] = pd.to_datetime(stock_data['日期'])
#         stock_data.set_index(['日期'], inplace=True)
#         stock_data.to_csv('./stock_data/daily/688327_daily.csv', mode='a', header=False, index=True)
# except:
#     df = ak.stock_zh_a_hist('688327', 'daily', '19000101', today, adjust='')
#     df.to_csv('./stock_data/daily/688327_daily.csv', mode='a', header=True, index=False)
# print(ak.stock_zh_a_spot_em())


# df_sql = pd.read_csv('./stock_data/daily/000001_daily.csv', index_col='日期', parse_dates=True,
#                              na_values=['nan', 'Nan', 'NaN', 'NaT', '', ''])
# df_sql_data = df_sql.tail(1)
# print(pd.to_datetime(df_sql_data.index))
# update_date = str(pd.to_datetime(df_sql_data.index) - datetime.timedelta(days=1))[16:26].replace("-", "")
#
# print(update_date)
# print(df_sql[update_date::])
# print(update_date > )

a = ['000502', '002111', '603717', '300931', '002072', '601116', '002985', '600797', '300737', '600039']
b = ['603717', '300931', '600039', '002072', '000502', '300737', '601116', '002985', '002111', '600797']
print(set(a) == set(b))

