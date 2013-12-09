from optparse import OptionParser
import csv
import simplejson

DELIM = '\t'

MULTIPLE_FEATURE_DELIM = ','

month_mapping = {"Jan":1, "Feb":2, "Mar":3, "Apr":4, "May":5, "Jun":6, "Jul":7, "Aug":8, "Sep":9, "Oct":10, "Nov":11, "Dec":12}

def parse_command_line():
    parser = OptionParser(usage="python lastFM_user_feature.py -u=<user-file> -a=<age_bin_interval> -dy=<date_bin_year> -dm=<date_bin_month> -dd=<date_bin_day>"
        + "[other optional options]")

    # Information about the user file
    parser.add_option("-u", "--user-file", action="store", type="string", dest="user_file",
                      help="The file containing the user features")

    # Information about the bin segmentation info
    parser.add_option("-a", "--age_bin_interval", action="store", type="int", dest="age_interval",default = 5,
                      help="The interval of an age bin")

    parser.add_option("-y", "--date_bin_year", action="store", type="int", dest="year_interval", default = 0,
                      help="The interval of date bin on year")

    parser.add_option("-m", "--date_bin_month", action="store", type="int", dest="month_interval", default = 0,
                      help="The interval of date bin on month")

    parser.add_option("-d", "--date_bin_day", action="store", type="int", dest="day_interval", default = 0,
                      help="The interval of date bin on day")

    return parser.parse_args()


def date_key_conversion(date, year_interval, month_interval, day_interval):
    date_format = date.replace(',',' ').split()
    year = int(date_format[2])
    month = month_mapping[date_format[0]]
    day = int(date_format[1])
    if day_interval != 0:
        key = str(year) + ' ' + str(month) + ' ' + str(day / day_interval)
    elif month_interval != 0:
        key = str(year) + ' ' + str(month / month_interval)
    elif year_interval != 0:
        key = str(year / year_interval)
    else: #Not specified, each day an independent bin
        key = str(year) +' ' + str(month) + ' ' + str(day)
    return key

def age_key_conversion(age, age_interval):
    if age == '':
        return age
    age_numeric = int(age)
    if age_interval != 0:
        key = str(age_numeric / age_interval)
    else:
        key = age
    return key

def parse_user_features(user_feature_file, age_interval, year_interval, month_interval, day_interval):

    with open(user_feature_file, 'r') as feature_file:
        user_out_file = open(user_feature_file + "_age"+ str(age_interval)+"_"+str(year_interval)+"y"+str(month_interval)+"m"+str(day_interval)+"d"+".user", 'w')
        reader = csv.reader(feature_file, delimiter=DELIM)
        for row in reader:

            age_key = age_key_conversion(row[2],age_interval)
            date_key = date_key_conversion(row[4], year_interval, month_interval, day_interval)
            out_str = row[0] + DELIM + row[1] + DELIM + age_key + DELIM + row[3] + DELIM + date_key

            #Write the out_str to the output file
            user_out_file.write(out_str + '\n')

if __name__ == "__main__":

    (options, args) = parse_command_line()
    #print options, args

    graph_info = parse_user_features(options.user_file, options.age_interval, options.year_interval, options.month_interval, options.day_interval)

