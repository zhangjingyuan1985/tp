# coding=UTF-8

import configparser
import psycopg2
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


cursor.execute("truncate table bigdata.t_bus_field_false_ratio")
cursor.execute("select table_catalog,table_schema,table_name,table_type from information_schema.tables where table_name like 't_bus_%';")
#cursor.execute("SELECT data_table,title from public.index_info")

list_table=[]
list_all_feild = []
for_count = 0
rows_table = cursor.fetchall()
for row_table in rows_table:
    table_catalog=row_table[0]
    table_schema = row_table[1]
    row_tablename=row_table[2]
    table_type=row_table[3]
#    print(row_tablename)
    row_tablename_str=str(row_tablename)
    sum_false1 = 0
    sum_false = 0
    sum_total = 0

#    cursor.execute("SELECT col_description(a.attrelid, a.attnum) as comment, format_type(a.atttypid,a.atttypmod) as type, a.attname as name, a.attnotnull as notnull FROM    pg_class as c, pg_attribute as a  where  c.relname = 'tablename' and a.attrelid = c.oid and a.attnum > 0")
    sql='''
    SELECT
	distinct(base."column_name"),
	col_description ( t1.oid, t2.attnum ),
	base.udt_name,
	COALESCE(character_maximum_length, numeric_precision, datetime_precision),
	(CASE
		WHEN ( SELECT t2.attnum = ANY ( conkey ) FROM pg_constraint WHERE conrelid = t1.oid AND contype = 'p' ) = 't' 
		THEN 1 ELSE 0 
	END ) 
FROM
	information_schema.COLUMNS base,
	pg_class t1,
	pg_attribute t2 
WHERE
	base."table_name" = '''+"'"+row_tablename+"'"+ '''
	AND t1.relname = base."table_name" 
	AND t2.attname = base."column_name" 
	AND t1.oid = t2.attrelid 
	AND t2.attnum > 0;
    '''
    cursor.execute(sql)
#    cursor.execute("select table_name,column_name,udt_name,character_maximum_length from information_schema.columns where table_schema='"+row_table[1]+"' and table_name='"+row_table[2]+"';")
    rows_fields = cursor.fetchall()
    print("current is " + str(for_count))
    list_all_feild.append(rows_fields)
    for_count = for_count + 1;
    for rows_field in rows_fields:
        field_name=rows_field[0]
        print(field_name)
        comment1=rows_field[1]
        type_value=rows_field[2]
#        print(type_value)
        character_maximum_length=rows_field[3]

        cursor.execute("select table_name,count(distinct (field)) from  bigdata.t_table_field1 group by table_name  having  table_name='"+row_tablename+"'")
        total=cursor.fetchall()
        for l in total:
#           print(total)
           total_value=l[1]
           print(total_value)

        sum_total = int(l[1])

   #     sql = "insert into bigdata.t_table_field1(table_name,field,type,create_time,table_catalog,table_schema,table_type,varchar_maxlength,comment) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
   #     parm = (row_tablename,field_name,type_value,date,table_catalog,table_schema,table_type,character_maximum_length,comment1)
        field_name_same="N"
        field_type_same = "N"
        field_length_same = "N"

        print(comment1)
        if comment1 == "公交线路名称":

            if field_name.encode(encoding="utf-8").strip() != "line_name".encode(encoding="utf-8").strip():
                print(field_name.encode(encoding="utf-8").strip())
                field_name_same = "Y"
                sum_false1 = sum_false1+1

            if type_value != "varchar":
                field_type_same = "Y"
                sum_false1 = sum_false1 + 1

            if character_maximum_length != 32:
                field_length_same = "Y"
                sum_false1 = sum_false1 + 1

        elif comment1 == "线路（不分方向）名称":
            if field_name != "route_name":
                field_name_same = "Y"
                sum_false1 = sum_false1 + 1

            if type_value != "varchar":
                field_type_same = "Y"
                sum_false1 = sum_false1 + 1

            if character_maximum_length != 100:
                field_length_same = "Y"
                sum_false1 = sum_false1 + 1

        elif comment1 == "线路（分方向）编号":
            if field_name != "route_id":
                field_name_same = "Y"
                sum_false1 = sum_false1 + 1

            if type_value != "varchar":
                field_type_same = "Y"
                sum_false1 = sum_false1 + 1

            if character_maximum_length != 32:
                field_length_same = "Y"
                sum_false1 = sum_false1 + 1

        elif comment1 == "站点编号":
            if field_name != "stop_id":
                field_name_same = "Y"
                sum_false1 = sum_false1 + 1

            if type_value != "varchar":
                field_type_same = "Y"
                sum_false1 = sum_false1 + 1

            if character_maximum_length != 32:
                field_length_same = "Y"
                sum_false1 = sum_false1 + 1

        elif comment1 == "断面字段":
            if field_name != "segment_id":
                field_name_same = "Y"
                sum_false1 = sum_false1 + 1

            if type_value != "varchar":
                field_type_same = "Y"
                sum_false1 = sum_false1 + 1

            if character_maximum_length != 65:
                field_length_same = "Y"
                sum_false1 = sum_false1 + 1
        else:continue

        if sum_false1 > 0:sum_false=sum_false+1

        sql1 = "insert into bigdata.t_bus_field_false_ratio(table_name,field,sum_false,sum_total,false_total_ratio,create_time,table_catalog,table_schema,table_type,field_type,field_name_same,field_type_same,field_length_same,character_maximum_length) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        parm = (row_tablename, field_name, sum_false, sum_total, sum_false/sum_total, date, table_catalog,table_schema, table_type, type_value,field_name_same,field_type_same,field_length_same, character_maximum_length)
        cursor.execute(sql1, parm)
#        sql1 = "insert into bigdata.t_bus_field_false_ratio(table_name,field,comment,sum_false,sum_total,false_total_ratio,create_time,table_catalog,table_schema,table_type,field_type,character_maximum_length) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#        parm=(row_tablename,field_name,comment,sum_false,sum_total,sum_false/sum_total,date,table_catalog,table_schema,table_type,comment1,type_value,character_maximum_length)
#        cursor.execute(sql, parm)
    conn.commit()
print(list_all_feild)
print(for_count)
#conn2=psycopg2.connect(database=database, user=user,password=password, host=host, port=port)


#    cursor.execute("insert into public.sql_api_diaodu_increase(id,table_name,content,endpoint,create_time,title_api,title_table_name) select null AS id, '"+row_tablename_str+"' as table_name ,content as content,endpoint as endpoint, current_timestamp,title as title_api,'"+table_title_str+"' as title_tablename from public.sql_api  where content like '% "+row_tablename_str+" %'")
#    cursor.execute("insert into public.sql_api_diaodu_increase(table_name,content,endpoint,version,create_time,title_api,title_table_name) select '" + row_tablename_str + "' as table_name ,content as content,endpoint as endpoint, version as version,current_timestamp,title as title_api,'" + table_title_str + "' as title_tablename from public.sql_api  where content like '% " + row_tablename_str + " %'")

cursor.close()


conn.close()