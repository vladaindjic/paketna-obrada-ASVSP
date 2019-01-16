#!/usr/bin/python

from operator import itemgetter
import sys

current_communication = ""
current_complaints_count = 0
current_days_count = 0


def days_per_communication(days_count, complaints_count):
    return float(days_count) / float(complaints_count)


def format_output(communication, days_count, complaints_count):
    # print(communication, days_count, complaints_count)
    return "%s,%f" % (current_communication, days_per_communication(current_days_count, current_complaints_count))


for line in sys.stdin:
    line = line.strip()
    communication, days_str = line.split("\t\t\t")
    days = 0
    try:
        days = int(days_str)
    except ValueError:
        raise Exception("This is bad: %s" % line)
    # this is the same word
    if current_communication == communication:
        current_complaints_count += 1
        current_days_count += days
    # this is new word
    else:
        # if we have previous word, print/stor it
        if current_communication:
            print(format_output(current_communication, current_days_count, current_complaints_count))

        current_communication = communication
        current_days_count = days
        current_complaints_count = 1

# The last one
if current_communication:
    print(format_output(current_communication, current_days_count, current_complaints_count))



# Razlika u rezultatima

# Email,7.683511
# Email,7.753351206434316

# Fax,4.741400
# Fax,4.7414002875788075

# Phone,4.217758
# Phone,4.217831914379522

# Postal mail,7.284907
# Postal mail,7.2850350776664445

# Referral,5.820434
# Referral,5.822537630246917

# Web,2.447701
# Web,2.4763219629234534
