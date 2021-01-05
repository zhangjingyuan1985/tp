import psycopg2
import time
date=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print(date)

database = 'transpaas_index'
user = 'bg_read'
password = 'ORVGEK2BgeUFMJlH'
host = '10.3.4.54'
port = '5432'


def is_in_poly(p, poly):
    """
    :param p: [x, y]
    :param poly: [[], [], [], [], ...]
    :return:
    """
    px, py = p
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
    cursor.execute("select value from bigdata.t_config where key='poly'")
    table_value = cursor.fetchone()
    poly = table_value[0]
    # kafka_dict = eval(str(table_value[0]))
    # print(kafka_dict)
    cursor.execute("select table_name from bigdata.t_table_field where field like '%lng%' or field like '%lat%'")
    table_value = cursor.fetchall()
    list2 = list(set(table_value))
    for i in list2:
        cursor.execute("select lng,lat from" bigdata"+"." + i[0])
        rows_results = cursor.fetchall()
        for j in rows_results:
            flag = is_in_poly(j, poly)

            sql1 = "insert into bigdata.t_table_lng_lat_shenzhen(table_name,lng,lat,is_ornot_shenzhen,create_time) values(%s,%s,%s,%s,%s)"
            parm = (str(i[0]), str(j[0]), str(j[1]), str(flag), date)
            print(parm)
            cursor.execute(sql1, parm)
            conn.commit()