import pandas as pd
from matplotlib.dates import date2num
import akshare as ak
import pymysql


def golden_death(df):
    df['time'] = date2num(df.index.to_pydatetime())
    df = df[["开盘", "收盘", "最高", "最低"]]
    # 上涨（涨停）
    # df[(df['收盘'] - df['开盘']) / df['开盘'] >= 0.1]
    # 下跌（跌停）
    # df[(df['开盘'] - df['收盘'].shift(1)) / df['收盘'].shift(1) <= -0.1]
    # 计算macd
    df['ma5'] = df['开盘'].rolling(5).mean()
    df['ma30'] = df['开盘'].rolling(30).mean()
    # 选取数据范围
    df = df['2010-01-04':]
    # 倒叙排列
    df = df[::-1]
    # 取1000个数据
    df = df[:1000]
    # 画图
    # df[['收盘', 'ma5', 'ma30']].plot()
    # plt.show()
    # 计算金叉死叉
    sr1 = df['ma5'] < df['ma30']
    sr2 = df['ma5'] >= df['ma30']
    death_cross = df[sr1 & sr2.shift(1)].index
    golden_cross = df[~(sr1 | sr2.shift(1))].index
    sr1 = pd.Series(1, index=golden_cross)
    sr2 = pd.Series(0, index=death_cross)
    sr = sr1.append(sr2).sort_index()
    # 回测
    first_money = 100000
    hold = 0
    money = first_money
    for i in range(0, len(sr)):
        p = df['开盘'][sr.index[i]]
        if sr.iloc[i] == 1:
            # 金叉
            buy = (money // (100 * p))
            hold += buy * 100
            money -= buy * 100 * p
        else:
            money += hold * p
            hold = 0

    p = df['开盘'][-1]
    now_money = hold * p + money
    return int(now_money - first_money)


if __name__ == '__main__':
    import os

    file_dir_daily = './stock_data/daily'
    os_code_list_daily = []
    for root, dirs, files in os.walk(file_dir_daily, topdown=False):
        for file in files:
            os_code_list_daily.append(file[:6])

    for i in os_code_list_daily:
        df = pd.read_csv('./stock_data/daily/{code}_daily.csv'.format(code=i), index_col='日期', parse_dates=True,
                         na_values=['nan', 'Nan', 'NaN', 'NaT', '', ''])

        price = golden_death(df)
        stock_individual_info_em_df = ak.stock_individual_info_em(symbol="{code}".format(code=i))
        stock_name = str(stock_individual_info_em_df.values[5][1])
        # 打开数据库连接
        db = pymysql.connect(host='localhost',
                             user='root',
                             password='707116148',
                             database='golden_ma')
        cursor = db.cursor()
        # SQL 插入语句
        sql = """INSERT INTO golden(stockname,stockcode,price)VALUES ('{stockname}','{stockcode}','{price}')""".format(
            stockname=stock_name, stockcode=i, price=price)
        try:
            # 执行sql语句
            cursor.execute(sql)
            # 提交到数据库执行
            db.commit()
        except:
            # 如果发生错误则回滚
            db.rollback()

        # 关闭数据库连接
        db.close()
