"""
This is a boilerplate pipeline 'data_engineering'
generated using Kedro 0.18.12
"""
#Libraries for data loading, data manipulation and data visualisation
from typing import List, Dict
import pandas as pd      
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime as dt
from datetime import datetime, timezone
import pandas as pd
import pytz
from datetime import date
#import parser
import pandas as pd
from sqlalchemy import create_engine

import pandas as pd


# nodes.py

import pandas as pd
from kedro.config import ConfigLoader
from kedro.extras.datasets.pandas import SQLQueryDataSet

###retieving keys and secret
conf_path = "conf/"
# Creating a ConfigLoader to load configuration
conf_loader = ConfigLoader(conf_path)
# Loading credentials from the configuration file
credentials = conf_loader.get("credentials*", "credentials*/**")
# Accessing the credentials
db_connection_string = credentials["db_credentials"]["con"]

def fetch_raw_data(my_sql_data_source) -> pd.DataFrame:
   
    # Create a SQLQueryDataSet object
    sql = """ SELECT showroom_name, `Product Bought`, order_date, monthly_gains, `ID`,
    CONCAT(first_name, ' ', middle_name, ' ', last_name) AS full_name,
    employment_status, gender, date_of_birth, civil_status, occupation, year_together, type_of_home,
    duration_of_residence, depend_on_you, level_of_education, number_of_children, `business type`,
    `Category`, actual_payment_date, actual_amount, `no repayments`, `expected repayments`,
    `total repayment`, `Downpayment`, `Amount Owing`, `last date of payment`, `last_exp_date`,
    `missed days`, DATEDIFF(`last_exp_date`, `last date of payment`) AS `days_btw_last_payment`,
    CASE
          WHEN (`no repayments` < `expected repayments` AND `last_exp_date` < CURDATE()) THEN 1
          ELSE 0
      END AS `default_status`
    FROM (
    SELECT branches.name AS 'showroom_name', products.name 'Product Bought', order_date, monthly_gains,
      customers.id AS 'ID', customers.first_name, customers.middle_name, customers.last_name,
      customers.employment_status, gender, date_of_birth, civil_status, occupation, year_together,
      type_of_home, duration_of_residence, depend_on_you, level_of_education, number_of_children,
      business_types.name as 'business type', order_types.name AS 'Category',
      amortizations.actual_payment_date, amortizations.actual_amount,
      COUNT(amortizations.actual_payment_date) AS 'no repayments',
      COUNT(amortizations.expected_amount) AS 'expected repayments',
      SUM(expected_amount) AS "total repayment", new_orders.down_payment AS "Downpayment",
      (SUM(CASE WHEN expected_payment_date > CURDATE() THEN expected_amount ELSE 0 END) - 
        SUM(CASE WHEN expected_payment_date < CURDATE() THEN actual_amount ELSE 0 END)) AS "Amount Owing",
      max(actual_payment_date) as 'last date of payment', max(expected_payment_date) as last_exp_date,
      datediff(max(actual_payment_date), expected_payment_date) as 'missed days',
      new_orders.id AS new_order_id
    FROM altaraone.customers
    INNER JOIN altaraone.new_orders ON new_orders.customer_id = customers.id
    INNER JOIN altaraone.products ON new_orders.product_id = products.id
    INNER JOIN altaraone.order_types ON new_orders.order_type_id = order_types.id
    INNER JOIN altaraone.business_types on business_types.id = new_orders.business_type_id
    INNER JOIN altaraone.sales_categories ON new_orders.sales_category_id = sales_categories.id
    INNER JOIN altaraone.amortizations ON amortizations.new_order_id = new_orders.id
    INNER JOIN altaraone.branches ON new_orders.branch_id = branches.id
    GROUP BY new_order_id
    ) AS subquery """

    data_set = SQLQueryDataSet(sql=sql, 
                               credentials={"con": db_connection_string})

    df = data_set.load()

    return df


def preprocess(df: pd.DataFrame) -> pd.DataFrame:

    #replacing nan values in number_of_children, year_together, monthly_gain
    df['number_of_children'] = df['number_of_children'].fillna(0)
    df['year_together'] = df['year_together'].fillna(0)
    monthly = df['monthly_gains'].median()
    df['monthly_gains'] = df['monthly_gains'].fillna(monthly)

    #split the product bought column 
    df['loan_amount'] = df['Product Bought'].str.replace('\D+', '')

    # #date preprocessing
    # if isinstance(dob_col, str) and 'T' in dob_col:
    #     df[dob_col] = parser.parse(dob_col).strftime('%Y-%m-%d')

    # # Convert all timestamps to UTC
    # df[dob_col] = pd.to_datetime(df[dob_col], errors='coerce', utc=True)
    # df[dob_col] = df[dob_col].where(df[dob_col].dt.year > 1900)
    # # Get current UTC timestamp
    # now_utc = pd.Timestamp.now(tz=pytz.UTC)
    # df[dob_col] = pd.to_datetime(df[dob_col])
    # df[dob_col] = df[dob_col].dt.tz_convert('UTC')

    # # Calculate age in years
    # df['Age'] = (now_utc - df[dob_col]).astype('<m8[Y]')

    #clean columns
    df['civil_status'] = df['civil_status'].replace({'Married': 'married', 'Single': 'single'})
    df['gender'] = df['gender'].replace({0: 'female', 1: 'male', 'Male': 'male', 'Female': 'female'})
    df['type_of_home'] = df['type_of_home'].replace({'Owned': 'owned', 'Rented': 'rented', 
                                                     'Family': 'family'})
    df['employment_status'] = df['employment_status'].replace('informal (business)', 'informal(business)')
    df['level_of_education'] = df['level_of_education'].replace({'Primary': 'primary', 
                                                                 'University': 'university', 
                                                                 'Secondary': 'secondary', 
                                                                 'Polytechnic': 'polytechnic',
                                                                 'Masters': 'masters', 
                                                                 ' Secondary': 'secondary',
                                                                 })
    # Replace numbers greater than 4 with '4+'
    df.loc[df['depend_on_you'] > 4, 'depend_on_you'] = 4
    df['dependant'] = df['depend_on_you']

    #drop columns
    df = df.drop(["Product Bought", "ID","last date of payment", "last_exp_date",
                  "missed days", "days_btw_last_payment", "full_name", 
                  "depend_on_you"], axis=1)
    
    return df











# def fetch_raw_data(my_sql_data_source) -> pd.DataFrame:
   
    # # Create a SQLQueryDataSet object
    # sql = """ SELECT showroom_name, `Product Bought`, order_date, monthly_gains, `ID`,
    # CONCAT(first_name, ' ', middle_name, ' ', last_name) AS full_name,
    # employment_status, gender, date_of_birth, civil_status, occupation, year_together, type_of_home,
    # duration_of_residence, depend_on_you, level_of_education, number_of_children, `business type`,
    # `Category`, actual_payment_date, actual_amount, `no repayments`, `expected repayments`,
    # `total repayment`, `Downpayment`, `Amount Owing`, `last date of payment`, `last_exp_date`,
    # `missed days`, DATEDIFF(`last_exp_date`, `last date of payment`) AS `days_btw_last_payment`,
    # CASE
    #       WHEN (`no repayments` < `expected repayments` AND `last_exp_date` < CURDATE()) THEN 1
    #       ELSE 0
    #   END AS `default_status`
    # FROM (
    # SELECT branches.name AS 'showroom_name', products.name 'Product Bought', order_date, monthly_gains,
    #   customers.id AS 'ID', customers.first_name, customers.middle_name, customers.last_name,
    #   customers.employment_status, gender, date_of_birth, civil_status, occupation, year_together,
    #   type_of_home, duration_of_residence, depend_on_you, level_of_education, number_of_children,
    #   business_types.name as 'business type', order_types.name AS 'Category',
    #   amortizations.actual_payment_date, amortizations.actual_amount,
    #   COUNT(amortizations.actual_payment_date) AS 'no repayments',
    #   COUNT(amortizations.expected_amount) AS 'expected repayments',
    #   SUM(expected_amount) AS "total repayment", new_orders.down_payment AS "Downpayment",
    #   (SUM(CASE WHEN expected_payment_date > CURDATE() THEN expected_amount ELSE 0 END) - 
    #     SUM(CASE WHEN expected_payment_date < CURDATE() THEN actual_amount ELSE 0 END)) AS "Amount Owing",
    #   max(actual_payment_date) as 'last date of payment', max(expected_payment_date) as last_exp_date,
    #   datediff(max(actual_payment_date), expected_payment_date) as 'missed days',
    #   new_orders.id AS new_order_id
    # FROM altaraone.customers
    # INNER JOIN altaraone.new_orders ON new_orders.customer_id = customers.id
    # INNER JOIN altaraone.products ON new_orders.product_id = products.id
    # INNER JOIN altaraone.order_types ON new_orders.order_type_id = order_types.id
    # INNER JOIN altaraone.business_types on business_types.id = new_orders.business_type_id
    # INNER JOIN altaraone.sales_categories ON new_orders.sales_category_id = sales_categories.id
    # INNER JOIN altaraone.amortizations ON amortizations.new_order_id = new_orders.id
    # INNER JOIN altaraone.branches ON new_orders.branch_id = branches.id
    # GROUP BY new_order_id
    # ) AS subquery """
    # credentials = {
    #     "con": "mysql+pymysql://emmanuel:7idgENo6m@altara.cytg9ltzydpw.us-east-1.rds.amazonaws.com:3306/altaraone"
    # }
    # data_set = SQLQueryDataSet(sql=sql,
    #                         credentials=credentials)

    # df = data_set.load()

    #sql_query_dataset = SQLQueryDataSet(sql=config['sql_query'], credentials=config['credentials'])

    # Load the data from the SQL database into a Pandas DataFrame
    #df = sql_query_dataset.load()

   # return df



