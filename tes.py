import datetime

import pandas as pd
from matplotlib.dates import date2num
import akshare as ak

import pymysql

# today = '20' + str(datetime.date.today().strftime('%y%m%d'))
# try:
#     df = pd.read_csv('./stock_data/daily/000001_daily.csv', index_col='日期', parse_dates=True,
#                      na_values=['nan', 'Nan', 'NaN', 'NaT', '', '']).tail(1)
#     # 更新起止时间
#     update_date = str(pd.to_datetime(df.index) + datetime.timedelta(days=1))[16:27].replace("-", "")

#     if today > df.index:
#         stock_data = ak.stock_zh_a_hist('000001', 'daily', update_date, today, adjust='')
#         stock_data['日期'] = pd.to_datetime(stock_data['日期'])
#         stock_data.set_index(['日期'], inplace=True)
#         # stock_data = pd.concat([df, stock_data])
#         stock_data.to_csv('./stock_data/daily/000001_daily.csv', mode='a', header=False, index=True)
# except:
#     df = ak.stock_zh_a_hist('000001', 'daily', '19000101', today, adjust='')
#     df.to_csv('./stock_data/daily/000001_daily.csv', mode='a', header=True, index=False)
print(ak.stock_zh_a_spot_em())