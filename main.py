from datetime import datetime

import pymysql
import random
from multiprocessing import Pool
import time

'''
CREATE DATABASE students CHARSET=utf8mb4;
CREATE TABLE `students` (
  `id` int unsigned NOT NULL AUTO_INCREMENT,
  `name` char(20) NOT NULL DEFAULT '',
  `age` char(3) NOT NULL DEFAULT '',
  `class` char(2) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `stu_index_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=14753752 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
'''

def mysql():
    conn = pymysql.connect(host='localhost',
                           port=3306,
                           user='root',
                           password='707116148',
                           db='students',
                           charset='utf8mb4')
    curses = conn.cursor(pymysql.cursors.DictCursor)
    return conn, curses


def insert_data(x):
    # conn, curses = mysql()
    # li = []
    # for i in range(1, 10000001):
    #     stu_age = random.randint(12, 15)
    #     stu_class = random.randint(1, 50)
    #     li.append(('学生{name_num}'.format(name_num=str(i)), '{age}'.format(age=str(stu_age)),
    #                '{class_num}'.format(class_num=str(stu_class))))
    # sql = "insert into students(name, age, class) values(%s,%s,%s);"
    # curses.executemany(sql, li)
    # # print(li)
    # conn.commit()
    # curses.close()
    # conn.close()
    # conn, curses = mysql()
    li = []
    for i in range(1, 10000000):
        tuition = random.randint(10000, 15000)
        gender_num = random.randint(0, 1)
        gender_li = ['男', '女']
        hobby_num = random.randint(0, 2)
        hobby_li = ['数学', '语文', '英语']
        li.append(('{tuition}'.format(tuition=str(tuition)), '{gender}'.format(gender=gender_li[gender_num]),
                   '{hobby}'.format(hobby=hobby_li[hobby_num])))
    sql = "insert into student_info(tuition, gender, hobby) values(%s,%s,%s);"
    # curses.executemany(sql, li)
    # # print(li)
    # conn.commit()
    # curses.close()
    # conn.close()


if __name__ == '__main__':
    starttime = datetime.now()
    insert_data(1)
    # p = Pool(processes=10)
    # p.map(insert_data, range(10))
    # p.close()
    # p.join()
    endtime = datetime.now()
    print((endtime - starttime).seconds)
