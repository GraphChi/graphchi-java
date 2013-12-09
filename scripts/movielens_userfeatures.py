import sys
import csv

DELIM = "|"
OUT_DELIM = '\t'

if __name__ == "__main__":
    
    with open(sys.argv[1], 'r') as user_file:
        out_file = open(sys.argv[1] + ".processed", 'w')
        reader = csv.reader(user_file, delimiter=DELIM)
        for row in reader:
            out_str = row[0] + OUT_DELIM + str(int(row[1])/5) + OUT_DELIM + row[2] + OUT_DELIM + row[3]
            out_file.write(out_str + '\n')
    
        