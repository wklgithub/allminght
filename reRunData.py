#! /usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import os
import sys

script = sys.argv[1]  # 执行脚本
start = sys.argv[2]  # 开始时间
end = sys.argv[3]  # 结束时间

# 默认每次任务执行三个小时数据
if len(sys.argv) < 5:
    gap: float = 3  # hour
else:
    gap: float = float(sys.argv[4])

if len(start) == 19:
    dateFormatStr = "%Y-%m-%d %H:%M:%S"
elif len(start) == 13:
    dateFormatStr = "%Y-%m-%d %H"
elif len(start) == 10:
    dateFormatStr = "%Y-%m-%d"
else:
    print("日期格式错误！")
    sys.exit()


def fun():
    dateStart = datetime.datetime.strptime(start, dateFormatStr)
    dateEnd = datetime.datetime.strptime(end, dateFormatStr)
    while dateStart < dateEnd:
        start_day = dateStart + datetime.timedelta(hours=1)
        stop_day = dateStart + datetime.timedelta(hours=gap)
        if stop_day > dateEnd:
            stop_day = dateEnd
        cmd = 'sh {0} "{1}" "{2}"'.format(script, start_day, stop_day)
        os.system(cmd)
        print(cmd)
        dateStart = stop_day


if __name__ == "__main__":
    fun()
