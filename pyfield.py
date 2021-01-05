# coding=UTF-8

import configparser
import psycopg2
import time

database = "transpaas_index"
user = "bg_read"
password = "ORVGEK2BgeUFMJlH"
host = "10.3.4.54"
port = "5432"


database2="transpaas_index"

user2="bg_read"
password2="ORVGEK2BgeUFMJlH"
host2="10.3.4.54"
port2="5432"
print(port2)

date=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
conn = psycopg2.connect(database=database, user=user,password=password, host=host, port=port )
cursor = conn.cursor()


cursor.execute("TRUNCATE bigdata.t_table_field")
cursor.execute("select relname as TABLE_NAME, reltuples as rowCounts from pg_class where relkind = 'r' and relnamespace = (select oid from pg_namespace where nspname='public') order by rowCounts desc; ")
#cursor.execute("SELECT data_table,title from public.index_info")

list_table=[]
rows_table = cursor.fetchall()
for row_table in rows_table:

    row_tablename=row_table[0]
    table_title=row_table[1]
    print(row_tablename)
    print(table_title)
    row_tablename_str=str(row_tablename)
    table_title_str=str(table_title)
    cursor.execute("select a.attname as fieldname, col_description(a.attrelid,a.attnum) as comment,format_type(a.atttypid,a.atttypmod) as type, a.attnotnull as notnull from pg_class as c,pg_attribute as a where c.relname = '"+row_tablename+"' and a.attrelid = c.oid and a.attnum > 0")
    rows_fields = cursor.fetchall()
    for rows_field in rows_fields:
        field_name=rows_field[0]
        print(field_name)
        comment_value=rows_field[1]
        type_value=rows_field[2]
        print(type_value)
        not_ornot=rows_field[3]
        print(rows_field)



        sql = "insert into bigdata.t_table_field(table_name,field,type,create_time,comment,null_ornot) values(%s,%s,%s,%s,%s,%s)"
        parm = (row_tablename,field_name,type_value,date,comment_value,not_ornot)
        cursor.execute(sql, parm)
        conn.commit()





#conn2=psycopg2.connect(database=database, user=user,password=password, host=host, port=port)


#    cursor.execute("insert into public.sql_api_diaodu_increase(id,table_name,content,endpoint,create_time,title_api,title_table_name) select null AS id, '"+row_tablename_str+"' as table_name ,content as content,endpoint as endpoint, current_timestamp,title as title_api,'"+table_title_str+"' as title_tablename from public.sql_api  where content like '% "+row_tablename_str+" %'")
#    cursor.execute("insert into public.sql_api_diaodu_increase(table_name,content,endpoint,version,create_time,title_api,title_table_name) select '" + row_tablename_str + "' as table_name ,content as content,endpoint as endpoint, version as version,current_timestamp,title as title_api,'" + table_title_str + "' as title_tablename from public.sql_api  where content like '% " + row_tablename_str + " %'")


cursor.close()


conn.close()