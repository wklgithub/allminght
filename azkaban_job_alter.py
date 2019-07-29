#!/usr/bin/python
# -*- coding: utf-8 -*-

import MySQLdb
import pickle
import time
import socket
import struct
import os

now_time=int(time.time())*1000

data = []
def get_data(start_time,end_time):
    db = MySQLdb.connect("host", "user", "password", "azkaban", charset='utf8' )
    cursor = db.cursor()
    sql = "select projects.name,execution_jobs.flow_id,execution_jobs.job_id,execution_jobs.end_time,execution_jobs.status " \
          "from execution_jobs left join projects on execution_jobs.project_id=projects.id where execution_jobs.end_time<={0} and execution_jobs.end_time>={1}".format(end_time,start_time)
    cursor.execute(sql)
    results = cursor.fetchall()
    re_status = 0
    for row in results:
        project_name = row[0]
        flow_id = row[1]
        job_id = row[2]
        job_time = row[3]
        status = row[4]
        header = "azkaban."+str(project_name)+"."+str(flow_id)+"."+str(job_id)
        if status == 50:
            re_status = 1
        else:
            re_status = -1
        message = (header,(job_time/1000,re_status))
        data.append(message)
    db.close()

#get_data(t*1000)

def send_message():
     payload = pickle.dumps(data, protocol=2)
     header = struct.pack("!L", len(payload))
     message = header + payload

     sock = socket.create_connection(('ip', 2004), 5)
     sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
     try:
         sock.send(message)
     finally:  # sockets don't support "with" statement on Python 2.x
         sock.close()

time_dir = os.path.dirname(os.path.abspath(__file__))
try:
    with open(time_dir+'/time.txt','r') as f:
        last_timestamp = f.read()
        get_data(int(last_timestamp),int(now_time))
        send_message()
        #print data
except:
    get_data(int(now_time)-300000,int(now_time))
    send_message()

new_timestamp = str(int(now_time))

with open(time_dir+'/time.txt','w') as f:
    f.write(new_timestamp)

f.close()
