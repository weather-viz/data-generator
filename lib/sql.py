import psycopg2
import re
from datetime import datetime

class Database:
    timestamp_column_name = 'valid_time_gmt'
    columns = [
        ('valid_time_gmt', 'TIMESTAMP'),
        ('temp', 'FLOAT'),
        ('heat_index', 'FLOAT'),
        ('wc', 'FLOAT'),
        ('feels_like', 'FLOAT'),
        ('dewPt', 'FLOAT'),
        ('rh', 'FLOAT'),
        ('pressure', 'FLOAT'),
        ('precip_total', 'FLOAT'),
        ('precip_hrly', 'FLOAT'),
        ('snow_hrly', 'FLOAT'),
        ('wspd', 'FLOAT'),
        ('wdir', 'FLOAT'),
    ]

    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port=8812,
            user="admin",
            password="quest",
            dbname="qdb"  # QuestDB doesn't use database names, but psycopg2 requires this parameter
        )

    def get_table_name(station_code):
        return f"weather_{re.sub(r'[^a-zA-Z0-9_]', '_', station_code)}"

    def create_table(self, station_code):
        with self.conn.cursor() as cursor:
            table_name = Database.get_table_name(station_code)
            columns_str = ', '.join([f"{col} {dtype}" for col, dtype in self.columns])
            query = f"CREATE TABLE IF NOT EXISTS {table_name} \
                ({columns_str}) \
                TIMESTAMP({Database.timestamp_column_name}) \
                PARTITION BY DAY \
                DEDUP UPSERT KEYS({Database.timestamp_column_name})"
            cursor.execute(query)
            print(f"Table for {station_code} created successfully")

    def insert_data(self, station_code, data):
        columns = [name for name, _ in self.columns]
        columns_str = ', '.join(columns)
        table_name = Database.get_table_name(station_code)
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({', '.join(['%s'] * len(self.columns))})"
        with self.conn.cursor() as cursor:
            values_list = []
            for row in data:
                values = []
                for name, type in self.columns:
                    if row[name] is None:
                        values.append(float('NaN'))
                    elif type == 'FLOAT':
                        values.append(float(row[name]))
                    elif type == 'TIMESTAMP':
                        values.append(datetime.fromtimestamp(row[name]))
                    else:
                        values.append(row[name])
                values_list.append(values)
            cursor.executemany(query, values_list)
        self.conn.commit()