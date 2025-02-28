#!/usr/bin/env python
# coding: utf-8

import schedule
import time
import datetime
import subprocess
import pandas as pd

def is_business_day():
    today = pd.Timestamp(datetime.date.today())
    # Generate business days for the current week
    start = today - pd.offsets.Day(today.weekday())
    end = start + pd.offsets.Day(6)
    business_days = pd.bdate_range(start=start, end=end)
    return today in business_days

def run_script(script_name):
    print(f"Running {script_name} at {datetime.datetime.now().strftime('%H:%M:%S')}")
    subprocess.run(["python", script_name], check=True)

def schedule_tasks():
    if is_business_day():
        schedule.every().day.at("15:01").do(run_script, "FullStockPricing_1.py")
        schedule.every().day.at("15:05").do(run_script, "Columns_2.py")
        schedule.every().day.at("15:06").do(run_script, "Predictions_3.py")
        schedule.every().day.at("15:07").do(run_script, "ModelEvaluation_4.py")
        print("Tasks scheduled for today.")
    else:
        print("Not a business day. No tasks scheduled.")

# Initial scheduling
schedule_tasks()
schedule.run_pending()




