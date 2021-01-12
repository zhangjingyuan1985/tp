import psycopg2
import time
date=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
print(date)

database = 'transpaas_index'
user = 'bg_read'
password = 'ORVGEK2BgeUFMJlH'
host = '10.3.4.54'
port = '5432'


conn = psycopg2.connect(database=database, user=user,password=password, host=host, port=port )
cursor = conn.cursor()

cursor.execute("TRUNCATE bigdata.t_not_same_t_b")
cursor.execute("select table_catalog,table_schema,table_name,table_type,field,type,varchar_maxlength from bigdata.t_table_field;")
#cursor.execute("SELECT data_table,title from public.index_info")

list_table=[]
rows_table = cursor.fetchall()
for row_table in rows_table:
    table_catalog=row_table[0]

    table_schema = row_table[1]
    row_tablename=row_table[2]
    table_type=row_table[3]
    print(row_tablename)
    field=row_table[4]
    if field =="line_name":

    row_tablename_str=str(row_tablename)
    cursor.execute("select table_name,column_name,udt_name,character_maximum_length from information_schema.columns where table_schema='" +row_table[1] + "' and table_name='" + row_table[2] + "';")
    rows_fields = cursor.fetchall()
    for rows_field in rows_fields:
        field_name = rows_field[1]
        print(field_name)

        type_value = rows_field[2]
        print(type_value)
        character_maximum_length = rows_field[3]

        sql = "insert into bigdata.t_table_field(table_name,field,type,create_time,table_catalog,table_schema,table_type,varchar_maxlength) values(%s,%s,%s,%s,%s,%s,%s,%s)"
        parm = (
        row_tablename, field_name, type_value, date, table_catalog, table_schema, table_type, character_maximum_length)
        cursor.execute(sql, parm)
        conn.commit()

