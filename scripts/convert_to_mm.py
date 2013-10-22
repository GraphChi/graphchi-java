from optparse import OptionParser
import csv
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
    parser = OptionParser(usage="python convert_to_mm.py -g=<graph-file> -e=<num_edges>"
        + "[other optional options]")
    
    # Information about the graph file
    parser.add_option("-g", "--graph-file", action="store", type="string", dest="graph_file", 
                      help="The file containing the graph(<inedge> <out-edge> <edge-val>")
    parser.add_option("-e", "--num_edges", action="store", type="int", dest="num_edges", 
                      help="Number of edges in the graph file")
    
    #Information about the user file
    parser.add_option("-u", "--user_file_info", action="store", type="string", dest="user_file_info", 
                      help=("Json String containing required information about the user feature file." +  
                      " The format of JSON is as follows: \n") + FEATURE_JSON_FORMAT)
    
    #Information about the item file
    parser.add_option("-i", "--item_file_info", action="store", type="string", dest="item_file_info", 
                      help=("Json String containing required information about the item feature file." +  
                      " The format of JSON is as follows: \n") + FEATURE_JSON_FORMAT)

    return parser.parse_args()


def  update_vertex_map_from_graph(graph_file_name, user_mapping, item_mapping):
    num_edges = 0
    #Go through the graph file and compute the user and item maps
    uniq_user_counter = len(user_mapping) + 1
    uniq_item_counter = len(item_mapping) + 1
    
    with open(graph_file_name, 'r') as graph_file:
        reader = csv.reader(graph_file, delimiter=DELIM)
        for row in reader:
            num_edges += 1
            user = user_mapping.get(row[0], None)
            if user is None:
                user_mapping[row[0]] = uniq_user_counter
                uniq_user_counter = uniq_user_counter + 1
            
            item = item_mapping.get(row[1], None)
            if item is None:
                item_mapping[row[1]] = uniq_item_counter
                uniq_item_counter = uniq_item_counter + 1

    return num_edges

def convert_to_matrix_market(graph_file_name, user_mapping, item_mapping):
    
    num_edges = update_vertex_map_from_graph(graph_file_name, user_mapping, item_mapping)
    
    with open(graph_file_name, 'r') as graph_file:
        out_file = open(graph_file_name + ".mm", 'w')
        out_file.write("%%MatrixMarket matrix coordinate real general\n");
        out_file.write("% Generated on <DATE>\n");
        out_file.write(str(len(user_mapping)) + ' ' + str(len(item_mapping)) + ' ' + str(num_edges) + '\n')
        
        reader = csv.reader(graph_file, delimiter=DELIM)
        for row in reader:
            user = user_mapping.get(row[0], None)
            if user is None:
                user = uniq_vertex_count
                user_mapping[row[0]] = user
                uniq_vertex_count = uniq_vertex_count + 1
            
            item = item_mapping.get(row[1], None)
            if item is None:
                item = item_count
                item_mapping[row[1]] = item
                uniq_item_count = item_count + 1
            
            out_file.write(str(user) + ' ' + str(item) + ' ' + row[2] + '\n')
    
    return {'num_edges': num_edges, 'num_features': 0}


def parse_vertex_features(vertex_mapping, feature_file_info_str):
    feature_file_info = simplejson.loads(feature_file_info_str)

    multiple_feature_delim = feature_file_info.get("multiple_feature_delim", MULTIPLE_FEATURE_DELIM)
    
    uniq_vertex_count = len(vertex_mapping) + 1
    feature_count = 1
    feature_mapping = {}
    
    with open(feature_file_info["file_name"], 'r') as feature_file:
        user_out_file = open(feature_file_info["file_name"] + ".conv", 'w')
        reader = csv.reader(feature_file, delimiter=DELIM)
        for row in reader:
            vertex = vertex_mapping.get(row[0], None)
            
            #If this vertex was not seen in the actual file.
            if vertex is None:
                vertex_mapping[row[0]] = uniq_vertex_count
                uniq_vertex_count += 1
                
            out_str = str(vertex_mapping[row[0]])
            
            for i in range(1, len(row)):
                if "delete_cols" in feature_file_info and i in feature_file_info.delete_cols:
                    continue

                #Add numerical attribute
                if "numerical_attr" in feature_file_info and i in feature_file_info.numerical_attr:
                    feature_label = feature_mapping.get((i, 0), None)
                    if feature_label is None:
                         feature_mapping[(i, 0)] = feature_count
                         feature_label = feature_count
                         feature_count += 1
                    out_str = out_str + DELIM + srt(feature_label) + ":" + row[i]
                    continue
                
                # Add categorical attribute
                feature_values = row[i].split(multiple_feature_delim)
                for val in feature_values:
                    feature_label = feature_mapping.get((i, val), None)
                    if feature_label is None:
                         feature_mapping[(i, val)] = feature_count
                         feature_label = feature_count
                         feature_count += 1
                    out_str = out_str + DELIM + str(feature_label) + ":1"
            
            #Write the out_str to the output file
            user_out_file.write(out_str + '\n')
            
    return {'num_entries': len(vertex_mapping), 'num_features': feature_count}
    
    
if __name__ == "__main__":

    (options, args) = parse_command_line() 
    #print options, args
    
    user_mapping = {}
    users_info = {}
    if hasattr(options, "user_file_info"):
        users_info = parse_vertex_features(user_mapping, options.user_file_info)
    
    item_mapping = {}
    items_info = {}
    if hasattr( options, "item_file_info"):
        items_info = parse_vertex_features(item_mapping, options.item_file_info)
    
    graph_info = convert_to_matrix_market(options.graph_file, user_mapping, item_mapping)
    
    with open(options.graph_file + ".info", 'w') as f:
        f.write(
            simplejson.dumps( 
                {
                    'num_users': len(user_mapping),
                    'num_user_features': users_info.get('num_features', 0),
                    'num_items': len(item_mapping),
                    'num_item_features': items_info.get('num_features', 0),
                    'num_edge_features': graph_info.get('num_features', 0),
                    'num_edges': graph_info.get('num_edges', 0),
                    'user_mapping': user_mapping,
                    'item_mapping': item_mapping
                }
                )
            )
                          
