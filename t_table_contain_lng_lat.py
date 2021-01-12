import psycopg2
import time

date=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print(date)

database = 'transpaas_index'
user = 'bg_read'
password = 'ORVGEK2BgeUFMJlH'
host = '10.3.4.54'
port = '5432'

conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
cursor = conn.cursor()
cursor.execute("truncate table bigdata.t_table_contain_lng_lat")
cursor.execute("select table_name,field,table_schema,table_catalog,table_type from bigdata.t_table_field where field like '%lng%' union select table_name,field,table_schema,table_catalog,table_type from bigdata.t_table_field where field like '%lat' union select table_name,field,table_schema,table_catalog,table_type from bigdata.t_table_field where field like 'lat%'")
field_value = cursor.fetchall()
list2 = list(set(field_value))
for i in list2:
        print(i[0])
        print(i[1])
        sql1 = "insert into bigdata.t_table_contain_lng_lat(table_name,field,create_time,table_schema,table_catalog,table_type) values(%s,%s,%s,%s,%s,%s)"
        parm = (str(i[0]), str(i[1]),date,str(i[2]),str(i[3]),str(i[4]) )
        print(parm)
        cursor.execute(sql1, parm)
conn.commit()
cursor.close()
conn.close()