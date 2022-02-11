import pymysql
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

def exec_cron():

    db = pymysql.connect(
        host=host,
        user=user,
        password=password,
        charset='utf8mb4',
        database=database
    )

    now = datetime.now()
    fd = now.strftime('%Y-%m-%d %H:%M:%S')
    curs = db.cursor()

    sql = "select price from fbase_btc where symbol =%s and dt between %s and %s"
    isql = """insert into fchart(symbol, rods, dt, open, close, high, low)
                values (%s, %s, %s, %s, %s, %s, %s)"""
    jsql = """insert into fchart_bak(symbol, rods, dt, open, close, high, low)
                values (%s, %s, %s, %s, %s, %s, %s)"""

    # 5min
    if int(now.strftime('%M')) in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]:
        b_now = now - timedelta(minutes = 5)
        b_fd = b_now.strftime('%Y-%m-%d %H:%M:%S')

        curs.execute(sql, ('BTCUSDT', b_fd, fd))
        rows = curs.fetchall()
        d_ochl = ochl(rows, 5)

        curs.execute(isql, ('BTCUSDT', 'min5', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        curs.execute(jsql, ('BTCUSDT', 'min5', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        db.commit()    

    # 15min
    if int(now.strftime('%M')) in [0, 15, 30, 45]:
        b_now = now - timedelta(minutes = 15)
        b_fd = b_now.strftime('%Y-%m-%d %H:%M:%S')

        curs.execute(sql, ('BTCUSDT', b_fd, fd))
        rows = curs.fetchall()
        d_ochl = ochl(rows, 15)
        
        curs.execute(isql, ('BTCUSDT', 'min15', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        curs.execute(jsql, ('BTCUSDT', 'min15', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        db.commit()    

    if int(now.strftime('%M')) in [0, 30]:
        b_now = now - timedelta(minutes = 30)
        b_fd = b_now.strftime('%Y-%m-%d %H:%M:%S')

        curs.execute(sql, ('BTCUSDT', b_fd, fd))
        rows = curs.fetchall()
        d_ochl = ochl(rows, 30)

        curs.execute(isql, ('BTCUSDT', 'min30', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        curs.execute(jsql, ('BTCUSDT', 'min30', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        db.commit()    

    if int(now.strftime('%M')) in [0]:
        b_now = now - timedelta(minutes = 60)
        b_fd = b_now.strftime('%Y-%m-%d %H:%M:%S')

        curs.execute(sql, ('BTCUSDT', b_fd, fd))
        rows = curs.fetchall()
        d_ochl = ochl(rows, 60)

        curs.execute(isql, ('BTCUSDT', 'hour1', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        curs.execute(jsql, ('BTCUSDT', 'hour1', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        db.commit()   

    if int(now.strftime('%M')) in [0] and int(now.strftime('%H')) in [0, 4, 8, 12, 16, 20]:
        b_now = now - timedelta(minutes = 240)
        b_fd = b_now.strftime('%Y-%m-%d %H:%M:%S')

        curs.execute(sql, ('BTCUSDT', b_fd, fd))
        rows = curs.fetchall()
        d_ochl = ochl(rows, 240)

        curs.execute(isql, ('BTCUSDT', 'hour4', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        curs.execute(jsql, ('BTCUSDT', 'hour4', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        db.commit()   
    
    if int(now.strftime('%M')) in [0] and int(now.strftime('%H')) in [0]:
        b_now = now - timedelta(minutes = 1440)
        b_fd = b_now.strftime('%Y-%m-%d %H:%M:%S')

        curs.execute(sql, ('BTCUSDT', b_fd, fd))
        rows = curs.fetchall()
        d_ochl = ochl(rows, 1440)

        curs.execute(isql, ('BTCUSDT', 'day1', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        curs.execute(jsql, ('BTCUSDT', 'day1', b_fd, float(d_ochl['open']), float(d_ochl['close']), float(d_ochl['high']), float(d_ochl['low'])))
        db.commit()   

    db.close()

def ochl(rows, num):
    i = 1
    ochl_dict = dict()
    for row in rows:
        if i == 1:
            ochl_dict['open'] = row[0]
            ochl_dict['high'] = row[0]
            ochl_dict['low'] = row[0]
        else:
            if ochl_dict['high'] < row[0]:
                ochl_dict['high'] = row[0]
            elif ochl_dict['low'] > row[0]:
                ochl_dict['low'] = row[0]
            else:
                pass
        if i == len(rows):
            ochl_dict['close'] = row[0]
        i+=1
    return ochl_dict

sched = BlockingScheduler()
sched.add_job(exec_cron, 'cron', minute='*/5')

sched.start()
