import git
import os
import pandas as pd
import json
import requests
import psycopg2 as pg
from git import Repo

# cloning github respository and making connection
def git_connect():
    try:
        git.Repo.clone_from(
            'https://github.com/PhonePe/pulse.git', 'Data')
    except Exception as e:
        print(f"❌ Error: {e}")

# data extraction from the gihub repo

class data_extract:
    def aggregated_transaction():
        try:
            path = 'Phonepe_EDA/Data/data/aggregated/transaction/country/india/state'
            agg_state_list = os.listdir(path)

            data = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [],'Transaction_count': [], 'Transaction_amount': []}

            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year = os.listdir(path_i)

                for j in agg_year:
                    path_j = path_i + j + "/"
                    Agg_yr_list = os.listdir(path_j)

                    for k in Agg_yr_list:
                        path_k = path_j + k
                        file = open(path_k, "r")
                        d = json.load(file)

                        for z in d['data']['transactionData']:
                            type = z['name']
                            count = z['paymentInstruments'][0]['count']
                            amount = z['paymentInstruments'][0]['amount']

                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['Transaction_type'].append(type)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)

            return data

        except Exception as e:
            print(f"❌ Error: {e}")

    def aggregated_user():
        try:
            path = "Phonepe_EDA/Data/data/aggregated/user/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [],'User_brand': [], 'User_count': [], 'User_percentage': []}

            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year = os.listdir(path_i)

                for j in agg_year:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)

                    for k in agg_year_json:
                        path_k = path_j + k
                        file = open(path_k, 'r')
                        d = json.load(file)

                        try:
                            for z in d['data']['usersByDevice']:
                                brand = z['brand']
                                count = z['count']
                                percentage = (z['percentage']*100)

                                data['State'].append(i)
                                data['Year'].append(j)
                                data['Quarter'].append(k[0])
                                data['User_brand'].append(brand)
                                data['User_count'].append(count)
                                data['User_percentage'].append(percentage)

                        except:
                            pass

            return data

        except Exception as e:
            print(f"❌ Error: {e}")

    def map_transaction():
        try:
            path = "Phonepe_EDA/Data/data/map/transaction/hover/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [], 'District': [],'Transaction_count': [], 'Transaction_amount': []}

            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year = os.listdir(path_i)

                for j in agg_year:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)

                    for k in agg_year_json:
                        path_k = path_j + k
                        f = open(path_k, 'r')
                        d = json.load(f)

                        for z in d['data']['hoverDataList']:
                            district = z['name'].split(' district')[0]
                            count = z['metric'][0]['count']
                            amount = z['metric'][0]['amount']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['District'].append(district)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)
            return data

        except:
            pass

    def map_user():
        try:
            path = "Phonepe_EDA/Data/data/map/user/hover/country/india/state/"
            agg_state_list = os.listdir(path)

            data = {'State': [], 'Year': [], 'Quarter': [],'District': [], 'Registered_user': [], 'App_opens': []}

            for i in agg_state_list:
                path_i = path + i + "/"
                agg_year = os.listdir(path_i)

                for j in agg_year:
                    path_j = path_i + j + "/"
                    agg_year_json = os.listdir(path_j)

                    for k in agg_year_json:
                        path_k = path_j + k
                        file = open(path_k, "r")
                        d = json.load(file)

                        for z_key, z_value in d['data']['hoverData'].items():
                            district = z_key.split(' district')[0]
                            reg_user = z_value['registeredUsers']
                            app_opens = z_value['appOpens']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['District'].append(district)
                            data['Registered_user'].append(reg_user)
                            data['App_opens'].append(app_opens)
            return data

        except:
            pass

    def top_transaction_district():
        try:
            path = "Phonepe_EDA/Data/data/top/transaction/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [], 'District': [],'Transaction_count': [], 'Transaction_amount': []}

            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year = os.listdir(path_i)

                for j in agg_year:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)

                    for k in agg_year_json:
                        path_k = path_j + k
                        f = open(path_k, 'r')
                        d = json.load(f)

                        for z in d['data']['districts']:
                            district = z['entityName']
                            count = z['metric']['count']
                            amount = z['metric']['amount']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['District'].append(district)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)
            return data
        except:
            pass

    def top_transaction_pincode():
        try:
            path = "Phonepe_EDA/Data/data/top/transaction/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],'Transaction_count': [], 'Transaction_amount': []}

            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year = os.listdir(path_i)

                for j in agg_year:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)

                    for k in agg_year_json:
                        path_k = path_j + k
                        f = open(path_k, 'r')
                        d = json.load(f)

                        for z in d['data']['pincodes']:
                            pincode = z['entityName']
                            count = z['metric']['count']
                            amount = z['metric']['amount']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['Pincode'].append(pincode)
                            data['Transaction_count'].append(count)
                            data['Transaction_amount'].append(amount)
            return data
        except:
            pass

    def top_user_district():
        try:
            path = "Phonepe_EDA/Data/data/top/user/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [],'District': [], 'Registered_user': []}

            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year = os.listdir(path_i)

                for j in agg_year:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)

                    for k in agg_year_json:
                        path_k = path_j + k
                        f = open(path_k, 'r')
                        d = json.load(f)
                        for z in d['data']['districts']:
                            district = z['name']
                            reg_user = z['registeredUsers']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['District'].append(district)
                            data['Registered_user'].append(reg_user)
            return data

        except:
            pass

    def top_user_pincode():
        try:
            path = "Phonepe_EDA/Data/data/top/user/country/india/state/"
            agg_state_list = os.listdir(path)
            data = {'State': [], 'Year': [], 'Quarter': [],'Pincode': [], 'Registered_user': []}

            for i in agg_state_list:
                path_i = path + i + '/'
                agg_year = os.listdir(path_i)

                for j in agg_year:
                    path_j = path_i + j + '/'
                    agg_year_json = os.listdir(path_j)
                    for k in agg_year_json:
                        path_k = path_j + k
                        f = open(path_k, 'r')
                        d = json.load(f)
                        for z in d['data']['pincodes']:
                            pincode = z['name']
                            reg_user = z['registeredUsers']
                            data['State'].append(i)
                            data['Year'].append(j)
                            data['Quarter'].append(k[0])
                            data['Pincode'].append(pincode)
                            data['Registered_user'].append(reg_user)
            return data

        except:
            pass
class data_transform:

    # transforming data related to transactions to pandas data frame & moving the data to CSV file
    aggregated_transaction = pd.DataFrame(data_extract.aggregated_transaction())
    aggregated_transaction.to_csv('aggregated_transaction.csv',index=False)
    map_transaction = pd.DataFrame(data_extract.map_transaction())
    map_transaction.to_csv('map_transaction.csv',index=False)
    top_transaction_district = pd.DataFrame(data_extract.top_transaction_district())
    top_transaction_district.to_csv('top_transaction_district.csv',index=False)
    top_transaction_pincode = pd.DataFrame(data_extract.top_transaction_pincode())
    top_transaction_pincode.to_csv('top_transaction_pincode.csv',index=False)

    # transforming data related to users to pandas data frame
    aggregated_users = pd.DataFrame(data_extract.aggregated_user())
    aggregated_users.to_csv('aggregated_users.csv',index=False)
    map_users = pd.DataFrame(data_extract.map_user())
    map_users.to_csv('map_users.csv',index=False)
    top_users_district = pd.DataFrame(data_extract.top_user_district())
    top_users_district.to_csv('top_users_district.csv',index=False)
    top_users_pincode = pd.DataFrame(data_extract.top_user_pincode())
    top_users_pincode.to_csv('top_users_pincode.csv',index=False)

# Create table function
def create_sql_table():


    # -------------------------------------
    # Database Configuration (update yours)
    # -------------------------------------
    # DB_CONFIG = {
    #     'host': 'dpg-d292oh7diees73fhkbt0',      
    #     'database': 'postgres',                                       # e.g., 'mydb'
    #     'user': 'postgres_online',                                      # e.g., 'mydb_user'
    #     'password': '1234',                           # Find in Render dashboard
    #     'port': 5342                                                              # Usually 5432
    # }

    # Creating connection with Postgres DB
    pgdb = pg.connect(host="dpg-d292oh7diees73fhkbt0",
                    user="postgres",
                    password="12334t",
                    database= "postgres"
                    )
    pgcursor = pgdb.cursor()

    # table for aggregated_transaction
    create_table_agg_trans = """
    create table if not exists aggregated_transaction(
    State    varchar(255),
    Year     int,
    Quarter  int,
    Transaction_type  varchar(255),
    Transaction_count bigint,
    Transaction_amount bigint
     );
     """

    # table for map_transaction
    create_table_map_trans = """
    create table if not exists map_transaction(
    State    varchar(255),
    Year     int,
    Quarter  int,
    District varchar(100),
    Transaction_count bigint,
    Transaction_amount bigint
    );
    """

    # table for top_transaction_district
    create_table_top_trans_dist = """
    create table if not exists top_transaction_district(
    State    varchar(255),
    Year     int,
    Quarter  int,
    District  varchar(255),
    Transaction_count bigint,
    Transaction_amount bigint
    );"""

    # table for top_transaction_pincode
    create_table_top_trans_pin = """create table if not exists top_transaction_pincode(
    State    varchar(255),
    Year     int,
    Quarter  int,
    Pincode  int,
    Transaction_count bigint,
    Transaction_amount bigint
    );
    """

    # table creation for users details

    # table for aggregated_users
    create_table_agg_users = """
    create table if not exists aggregated_users(
    State    varchar(255),
    Year     int,
    Quarter  int,
    User_brand  varchar(255),
    User_count bigint,
    User_percentage float
    );
    """

    # table for map_users
    create_table_map_users = """
    create table if not exists map_users(
    State    varchar(255),
    Year     int,
    Quarter  int,
    District  varchar(255),
    Registered_user bigint,
    App_opens bigint
    );
    """

    # table for top_users_district
    create_table_top_users_dist = """
    create table if not exists top_users_district(
    State    varchar(255),
    Year     int,
    Quarter  int,
    District  varchar(255),
    Registered_user bigint
    );
    """

    # table for top_users_pincode
    create_table_top_users_pin = """
    create table if not exists top_users_pincode(
    State    varchar(255),
    Year     int,
    Quarter  int,
    Pincode  int,
    Registered_user bigint
    );
    """

    # -------------------------------------
    # Execution
    # -------------------------------------
    try:
        # # 1. Connect to DB
        # conn = psycopg2.connect(**DB_CONFIG)
        # cursor = conn.cursor()

        # # 1. Create Tables
        pgcursor.execute(create_table_agg_trans)
        print("✅ Table 'aggregated_transaction' created or already exists.")
        pgcursor.execute(create_table_map_trans)
        print("✅ Table 'map_transaction' created or already exists.")
        pgcursor.execute(create_table_top_trans_dist)
        print("✅ Table 'top_transaction_district' created or already exists.")
        pgcursor.execute(create_table_top_trans_pin)
        print("✅ Table 'top_transaction_pincode' created or already exists.")
        pgcursor.execute(create_table_agg_users)
        print("✅ Table 'aggregated_users' created or already exists.")
        pgcursor.execute(create_table_map_users)
        print("✅ Table 'map_users' created or already exists.")
        pgcursor.execute(create_table_top_users_dist)
        print("✅ Table 'top_users_district' created or already exists.")
        pgcursor.execute(create_table_top_users_pin)
        print("✅ Table 'top_users_pincode' created or already exists.")

        pgdb.commit()

    except Exception as e:
      print(f"❌ Error: {e}")

    finally:
      if 'cursor' in locals():
          pgcursor.close()
      if 'conn' in locals():
          pgdb.close()
          print("✅ Connection closed.")

create_sql_table()

class insert_values_into_tables():

    def insert_to_aggregated_transaction(self,Conn_string):
        try:
          conn = pg.connect(Conn_string)
          cursor = conn.cursor()
          data = data_transform.aggregated_transaction.values.tolist()
          query = "insert into aggregated_transaction values(%s,%s,%s,%s,%s,%s)"
          print(query)
          for i in data:
              cursor.execute(query, tuple(i))
          print(f"Data inserted-{i}")
          conn.commit()
          cursor.close()
          conn.close()

        except Exception as e:
          print(f"❌ Error: {e}")

    # inserting values into map_transaction table
    def insert_to_map_transaction(self,Conn_string):
        try:
            conn = pg.connect(Conn_string)
            cursor = conn.cursor()
            data = data_transform.map_transaction.values.tolist()
            query = "insert into map_transaction values(%s,%s,%s,%s,%s,%s)"
            for i in data:
                cursor.execute(query, tuple(i))
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            print(f"❌ Error: {e}")

    # inserting values into top_transaction_district table
    def insert_to_top_transaction_district(self,Conn_string):
      try:
          conn = pg.connect(Conn_string)
          cursor = conn.cursor()
          data = data_transform.top_transaction_district.values.tolist()
          query = "insert into top_transaction_district values(%s,%s,%s,%s,%s,%s)"
          for i in data:
              cursor.execute(query, tuple(i))
          conn.commit()
          cursor.close()
          conn.close()
      except Exception as e:
          print(f"❌ Error: {e}")

    # inserting values into top_transaction_district table
    def insert_to_top_transaction_pincode(self,Conn_string):
      try:
          conn = pg.connect(Conn_string)
          cursor = conn.cursor()
          data = data_transform.top_transaction_pincode.values.tolist()
          query = "insert into top_transaction_pincode values(%s,%s,%s,%s,%s,%s)"
          for i in data:
              cursor.execute(query, tuple(i))
          conn.commit()
          cursor.close()
          conn.close()
      except Exception as e:
          print(f"❌ Error: {e}")

    # inserting values into users tables

     # inserting values into aggregated users table

    def insert_to_aggregated_users(self,Conn_string):
      try:
          conn = pg.connect(Conn_string)
          cursor = conn.cursor()
          data = data_transform.aggregated_users.values.tolist()
          data
          query = "insert into aggregated_users values(%s,%s,%s,%s,%s,%s)"
          for i in data:
              cursor.execute(query, tuple(i))
          conn.commit()
          cursor.close()
          conn.close()
      except Exception as e:
          print(f"❌ Error: {e}")

    # inserting values into  map_users table
    def insert_to_map_users(self,Conn_string):
      try:
          conn = pg.connect(Conn_string)
          cursor = conn.cursor()
          data = data_transform.map_users.values.tolist()
          query = "insert into map_users values(%s,%s,%s,%s,%s,%s)"
          for i in data:
              cursor.execute(query, tuple(i))
          conn.commit()
          cursor.close()
          conn.close()
      except Exception as e:
          print(f"❌ Error: {e}")

    # inserting values into  top_users_district table
    def insert_to_top_users_district(self,Conn_string):
      try:
          conn = pg.connect(Conn_string)
          cursor = conn.cursor()
          data = data_transform.top_users_district.values.tolist()
          query = "insert into top_users_district values(%s,%s,%s,%s,%s)"
          for i in data:
              cursor.execute(query, tuple(i))
          conn.commit()
          query
          print("done")
          cursor.close()
          conn.close()
      except Exception as e:
          print(f"❌ Error: {e}")

    # inserting values into  top_users_district table
    def insert_to_top_users_pincode(self,Conn_string):
      try:
          conn = pg.connect(Conn_string)
          cursor = conn.cursor()
          data = data_transform.top_users_pincode.values.tolist()
          query = "insert into top_users_pincode values(%s,%s,%s,%s,%s)"
          for i in data:
              cursor.execute(query, tuple(i))
          conn.commit()
          cursor.close()
          conn.close()
      except Exception as e:
          print(f"❌ Error: {e}")

#Insert extracted data to DB tables
def data_insertion_mysql():
    try:
        # -------------------------------------
        # Database Configuration (update yours)
        # -------------------------------------
        DB_CONFIG = {
            'host': 'dpg-d292oh7diees73fhkbt0',      # e.g., 'dpg-cn5v9u8l6cac73bs9ug0-a'
            'database': 'postgres',   # e.g., 'mydb'
            'user': 'postgres_online',            # e.g., 'mydb_user'
            'password': 'm2SQ6UPiVFM4HxteT',    # Find in Render dashboard
            'port': 5432
        }

        # 1. Connect to DB
        Conn_string=f"host={DB_CONFIG['host']} dbname={DB_CONFIG['database']} user={DB_CONFIG['user']} password={DB_CONFIG['password']} port={DB_CONFIG['port']}"
        # 2. Call class to Insert Data
        db=insert_values_into_tables()
        db.insert_to_aggregated_transaction(Conn_string)
        db.insert_to_map_transaction(Conn_string)
        db.insert_to_top_transaction_district(Conn_string)
        db.insert_to_top_transaction_pincode(Conn_string)
        db.insert_to_aggregated_users(Conn_string)
        db.insert_to_map_users(Conn_string)
        db.insert_to_top_users_district(Conn_string)
        db.insert_to_top_users_pincode(Conn_string)

    except Exception as e:
          print(f"❌ Error: {e}")


data_insertion_mysql()

#Inser Data from CSV file
class inser_from_csv():
    def insert_to_aggregated_transaction(self,Conn_string,csvpath):
        try:
          conn = psycopg2.connect(Conn_string)
          cursor = conn.cursor()
          data = pd.read_csv(csvpath)
          data = data.values.tolist()
          query = "insert into aggregated_transaction_1 values(%s,%s,%s,%s,%s,%s)"
          for i in data:
              cursor.execute(query, tuple(i))
          conn.commit()
          cursor.close()
          conn.close()

        except Exception as e:
          print(f"❌ Error: {e}")
def data_insertion_db():
    try:
        # -------------------------------------
        # Database Configuration (update yours)
        # -------------------------------------
        DB_CONFIG = {
            'host': RENDER_DB_HOST,      # e.g., 'dpg-cn5v9u8l6cac73bs9ug0-a'
            'database': RENDER_DB_NAME,   # e.g., 'mydb'
            'user': RENDER_DB_USER,            # e.g., 'mydb_user'
            'password': RENDER_DB_PASSWORD,    # Find in Render dashboard
            'port': RENDER_DB_PORT                     # Usually 5432
        }

        # 1. Connect to DB
        Conn_string=f"host={DB_CONFIG['host']} dbname={DB_CONFIG['database']} user={DB_CONFIG['user']} password={DB_CONFIG['password']} port={DB_CONFIG['port']}"
        # 2. Call class to Insert Data
        db=inser_from_csv()
        db.insert_to_aggregated_transaction(Conn_string,'aggregated_transaction.csv')
    except Exception as e:
          print(f"❌ Error: {e}")

data_insertion_db()
