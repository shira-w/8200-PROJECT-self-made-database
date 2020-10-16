# MY DATA BASE    ========
#=========================

import json
import dataclasses
import os.path

import db_api
from db_api import *  # importing is double to prevent errors

end_file_of_tables = ".json"


# AUXILIARY FUNCS============================================
def operators(current_value, operator, accepted_value):
    if operator == "<":
        return int(current_value) < int(accepted_value)
    if operator == ">":
        return int(current_value) > int(accepted_value)
    if operator == "=":
        return int(current_value) == int(accepted_value)
    if operator == ">=":
        return int(current_value) >= int(accepted_value)
    if operator == "<=":
        return int(current_value) <= int(accepted_value)
    print("QUERY_TABLE: UNDEFINED OPERATOR")


# =========================================================

# functions to handle any table: insert record, delete record, count records ect...
@dataclass
class DBTable(db_api.DBTable):

    def __init__(self, name, fields, key_field_name):
        super(DBTable, self).__init__(name, fields, key_field_name)
        self.counter = 0
        self.records = {}
        self.indexes = {}
        self.full_path = os.path.join(DB_ROOT, name + end_file_of_tables)

    def insert_record(self, values: Dict[str, Any]) -> None:
        primary_key = values.pop(self.key_field_name)
        if self.records.get(primary_key):
            raise ValueError("INSERT RECORD: record already exist")
        self.records.update({primary_key: values})
        self.counter += 1
        self.save_state()

    def delete_record(self, key: Any) -> None:
        result = self.records.pop(key, None)
        if result is None:
            raise ValueError("DELETE RECORD: not exist record")
        self.counter -= 1
        self.save_state()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        flag = 1
        for r in self.records.values():
            for c in criteria:
                # if
                if operators(r[c.field_name], c.operator, c.value) is False:
                    flag = 0
                    break
            if flag == 0:
                self.records.pop(r)
                self.counter -= 1
        self.save_state()

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        if self.records.get(key):
            self.records[key].update(values)
        else:
            raise ValueError("UPDATE RECORD: not exist record")
        self.save_state()

    def get_record(self, key: Any) -> Dict[str, Any]:
        if self.records.get(key):
            return self.records[key]
        else:
            raise ValueError("GET RECORD: not exist record")

    def count(self) -> int:
        return self.counter

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        set = {self.records.keys()}

        for c in criteria:
            if self.indexes.get(c.field_name) and c.value in self.indexes.values():
                set = set & self.indexes[c.field_name]
                # if set rek
            else:
                for r in self.records:
                    if operators(r[c.field_name], c.operator, c.value) is False:
                        set = set - {r}

        query_records = [self.records.keys()]
        for r in self.records:
            for c in criteria:
                if operators(r[c.field_name], c.operator, c.value) is False:
                    query_records.remove(r)
                    break

        return query_records

    def create_index(self, field_to_index: str) -> None:
        opposite_dict = {}
        for key in self.records.keys():
            value = self.records[key][field_to_index]
            if opposite_dict.get(value):
                opposite_dict[value].append(key)
            else:
                opposite_dict[value] = [key]
        self.indexes[field_to_index] = opposite_dict

    def save_state(self):
        with open(self.full_path, "w") as the_file:
            json.dump(self.records, the_file, default=str)


# functions to handle My Data Base: insert table, delete table, count tables ect...
@dataclass
class DataBase(db_api.DataBase):
    def __init__(self):
        self.counter = 0
        self.tables = {}
        if not os.listdir(DB_ROOT):
            return

        for file_name in os.listdir(DB_ROOT):
            full_path = os.path.join(DB_ROOT, file_name)
            records = {}
            if os.path.isfile(full_path) and os.access(full_path, os.R_OK):
                with open(full_path, "r", encoding="utf8") as f:
                    records = json.load(f)

            table_name = file_name[0:len(file_name) - 5]  # without .json
            fields = []
            for record_data in records.values():
                for name_field in record_data.keys():
                    fields.append(DBField(name_field, type(record_data[name_field])))
            if records != {}:
                dbt = DBTable(table_name, fields, fields[0].name)
            else:
                dbt = DBTable(table_name, [], "")
            dbt.counter += len(records)
            dbt.records = records
            self.tables[dbt.name] = dbt
            self.counter += 1

    def create_table(self, table_name: str, fields: List[DBField], key_field_name: str) -> DBTable:
        print("CREATE TABLE: " + table_name)
        print("KEY: " + key_field_name)
        print("FIELDS: ")
        for f in fields:
            print(f.name + ":" + str(f.type))

        full_path = f"{DB_ROOT}\\{table_name}.json"
        if os.path.isfile(full_path):
            full_path = f"{DB_ROOT}\\{table_name}1.json"

        with open(full_path, "w") as the_file:
            json.dump({}, the_file)
        self.tables[self.counter] = DBTable(table_name, fields, key_field_name)
        self.counter += 1
        return self.tables[self.counter - 1]

    def delete_table(self, table_name: str) -> None:
        if os.path.isfile(dbt.full_path) and os.access(dbt.full_path, os.R_OK) and self.tables.get(table_name):
            dbt = self.tables[table_name]
            os.remove(dbt.full_path)
            del self.tables[table_name]
            print(f"DELETE TABLE:"
                  f"NAME: {str}"
                  f"RECORDS AMOUNT:{dbt.counter}")
        else:
            raise ValueError("DELETE TABLE: undefined table")

    def num_tables(self) -> int:
        return self.counter

    # this function requires completion
    def query_multiple_tables(self, tables: List[str], fields_and_values_list: List[List[SelectionCriteria]],
                              fields_to_join_by: List[str]) -> List[Dict[str, Any]]:
        for t, f in zip(tables, fields_and_values_list):
            t.query_table(f)

    def get_tables_names(self) -> List[Any]:
        names = []
        for dvt in self.tables.values():
            names.append(dvt.name)
        return names

    def get_table(self, table_name: str) -> DBTable:
        return self.tables[self.get_tables_names().index(table_name)]

