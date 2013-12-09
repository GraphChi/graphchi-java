import sys
import csv

DELIM = "|"
OUT_DELIM = '\t'

if __name__ == "__main__":
    
    with open(sys.argv[1], 'r') as user_file:
        out_file = open(sys.argv[1] + ".processed", 'w')
        reader = csv.reader(user_file, delimiter=DELIM)
        for row in reader:
            out_str = row[0] + OUT_DELIM
            
            for i in range(5, len(row)):
                if row[i] == '1':
                    out_str = out_str + str(i) + ","
            if out_str[-1] == ',':
                out_str = out_str[:-1]
                
            out_file.write(out_str + '\n')