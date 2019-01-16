#!/usr/bin/python

import sys
import re
import csv
from datetime import date, datetime


def strip_and_remove_quotes(input_str):
    output_str = input_str.strip()
    output_str = output_str.replace('"', '')
    output_str = output_str.replace("'", "")
    return output_str


def convert_to_datetime(val):
    ret_val = datetime.strptime(val, "%m/%d/%Y")
    return ret_val


i = 0
for line in sys.stdin:
    line = line.strip()
    # ignore commas that are between double quotes
    parts = re.split(''',(?=(?:[^"]|"[^"]*")*$)''', line)
    received_date_str, sent_date_str, company = \
        (strip_and_remove_quotes(parts[0]), strip_and_remove_quotes(parts[13]), strip_and_remove_quotes(parts[12]))
    # this is just debug info
    i += 1
    if len(parts) != 18:
        print(i, line)
        raise Exception("%d, %s" % (i, line))

    # MM/dd/yyyy
    received_date = convert_to_datetime(received_date_str)
    sent_date = convert_to_datetime(sent_date_str)
    days_between = (sent_date - received_date).days
    # I hope that no one used three tabs in text
    print("%s\t\t\t%s" % (company, days_between))

    # print(str(received_date), str(sent_date))
