import psycopg2
import time
from numba import jit


import concurrent.futures
date=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print(date)

database = 'transpaas_index'
user = 'bg_read'
password = 'ORVGEK2BgeUFMJlH'
host = '10.3.4.54'
port = '5432'
fdate='20200928'


#@jit
def is_in_poly(p, poly):
    """
    :param p: [x, y]
    :param poly: [[], [], [], [], ...]
    :return
    """
    px, py = p
    px=float(px)
    py=float(py)


    is_in = False
    for i, corner in enumerate(poly):
        next_i = i + 1 if i + 1 < len(poly) else 0
        x1, y1 = corner
        x2, y2 = poly[next_i]
        if (x1 == px and y1 == py) or (x2 == px and y2 == py):  # if point is on vertex
            is_in = True
            break
        if min(y1, y2) < py <= max(y1, y2):  # find horizontal edges of polygon
            x = x1 + (py - y1) * (x2 - x1) / (y2 - y1)
            if x == px:  # if point is on edge
                is_in = True
                break
            elif x > px:  # if point is on left-side of line
                is_in = not is_in
    return is_in


if __name__ == '__main__':
    point = [3, 3]
#    poly = [[0, 0], [7, 3], [8, 8], [5, 5]]
#    poly=[[ 113.7519, 22.7223 ], [ 113.8857, 22.4532 ], [ 114.4124, 22.6030 ], [ 114.5084, 22.4413 ], [ 114.6289, 22.5130 ]  ,[114.5034,22.5781],[114.6026,22.6536],[114.4383,22.6992],[114.4009,22.7930],[114.1824,22.8231],[114.1685,22.6853],[113.8716,22.8634],[ 113.7519, 22.7223 ]]

    table_dict = {}
    conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
    cursor = conn.cursor()
    cursor.execute("truncate table bigdata.t_lng_lat_is_ornot_shenzhen_t_tencent_area_people_heat_ratio")
    cursor.execute("select value from bigdata.t_config where key='poly'")
    table_value = cursor.fetchone()
    poly1 = table_value[0]
    print(poly1)
    poly=list
    poly = eval(poly1)
    print(poly)
    sum_total = 0
    sum_false = 0
    cursor.execute("select value from bigdata.t_config where key='t_tencent_area_people_heat_date'")
    date_value=cursor.fetchone()
    date_list=[]
    date_list=eval(date_value[0])
    print(date_list)
    for date_value1 in date_list:
#        if i[3]=="t_tencent_area_people_heat":
#            continue
           cursor.execute("select lng,lat  from public.t_tencent_area_people_heat where city ='440300' and fdate = '"+str(date_value1)+"'")
           rows_results = cursor.fetchall()
           for j in rows_results:
               sum_total = sum_total + 1
               print(j)
               list_j=list(j)
               print(list_j)
               try:flag = is_in_poly(list_j, poly)
               except:flag=False
               else:flag = is_in_poly(list_j, poly)
               if flag == False:
                  sum_false = sum_false + 1
           float_ratio = float(sum_false) / float(sum_total)
           sql1 = "insert into bigdata.t_lng_lat_is_ornot_shenzhen_t_tencent_area_people_heat_ratio(table_catalog,table_schema,table_type,table_name,sum_false,false_ratio,create_time,sum_total,fdate) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
           parm = ('transpaas_index', 'bigdata','BASE TABLE','t_tencent_area_people_heat',str(sum_false), str(float_ratio), date, sum_total,date_value1)
           print(parm)
           cursor.execute(sql1, parm)
           conn.commit()
    cursor.close()
    conn.close()