from optparse import OptionParser
import csv
import sys
import simplejson

DELIM = '\t'

FEATURE_JSON_FORMAT = ("{\n"+
    "    file_name : <file_location>\n"+
    "    delim : <delimiter, default = \\t>\n"+
    "    num : <num_users>\n"+
    "    delete_cols :  [<List of columns to not consider>]\n"+
    "    multiple_feature_delim : <default = ','>\n"+
    "    numerical_attr : [<list of numerical attributes>]\n"
    "}")

MULTIPLE_FEATURE_DELIM = ','


def parse_command_line():
    parser = OptionParser(usage="python convert_Yahoo_to_mm.py -g=<graph-file> -e=<num_edges>"
        + "[other optional options]")

    # Information about the graph file
    parser.add_option("-g", "--graph-file", action="store", type="string", dest="graph_file",
                      help="The file containing the graph(<inedge> <out-edge> <edge-val>")

    return parser.parse_args()


def format_time(time):
    [hour, minute, second] = time.split(':')
    return int(hour)*3600+int(minute)*60 + int(second)

def convert_to_matrix_market(graph_file_name) :
    num_user = 1000990
    num_item = 624961
    num_rating = 262810175
    with open(graph_file_name, 'r') as graph_file:
        out_file = open(graph_file_name + ".mm", 'w')
        out_file.write("%%MatrixMarket matrix coordinate real general\n");
        out_file.write("% Generated on <DATE>\n");
        out_file.write(str(num_user) + ' ' + str(num_item) + ' ' + str(num_rating) + '\n')

        while 1:
            user_row = graph_file.readline()
            if not user_row:
                break
            [user_id, user_num_rating] = user_row.rstrip().split('|')

            for i in range(0,int(user_num_rating)):
                rating_row = graph_file.readline()
                [item_id, rating, date, time] = rating_row.rstrip().split(DELIM)
                formatted_time = format_time(time)
                out_str = user_id + DELIM + item_id + DELIM + rating + DELIM + date + DELIM + str(formatted_time) + '\n'
                out_file.write(out_str)
    out_file.close()

if __name__ == "__main__":

    (options, args) = parse_command_line()
    print options, args

    graph_info = convert_to_matrix_market(options.graph_file)
