import db
from db import *
from test_db import create_students_table
list = [DBField("id", int), DBField("first name", str), DBField("last name", str), DBField("place", int)]
sdb = DataBase()
dbt = sdb.create_table("fighters", list, "id")
dbt.insert_record(
    {dbt.fields[0].name: 99089, dbt.fields[1].name: "shira", dbt.fields[2].name: "wein", "age": 15})
dbt.insert_record(
    {dbt.fields[0].name: 667, dbt.fields[1].name: "dvora", dbt.fields[2].name: "zahzv", "age": 15})
dbt.insert_record(
    {dbt.fields[0].name: 455, dbt.fields[1].name: "maya", dbt.fields[2].name: "jo", "age": 4})
dbt.insert_record(
    {dbt.fields[0].name: 67, dbt.fields[1].name: "tirtsa", dbt.fields[2].name: "mor", "age": 19})
dbt.save_state()
dbt.create_index("age")
dbt.delete_record(99089)

