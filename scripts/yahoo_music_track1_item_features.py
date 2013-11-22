import csv
import sys
from sets import Set
from optparse import OptionParser

data_directory = '/home/shuhaoyu/Documents/CMU_semester_III/Capstone/Capstone_workspace/dataset/YahooMusic/ydata-ymusic-kddcup-2011-track1/'
track_data = 'trackData1.txt'
album_data = 'albumData1.txt'
artist_data = 'artistData1.txt'
genre_data = 'genreData1.txt'
DELIM = '|'
TAB_DELIM = '\t'
num_feature = 0
def parse_command_line():
    parser = OptionParser(usage="python yahoo_music_track1_item_features.py -o=<output-file> "
        + "[other optional options]")

    # Information about the graph file
    parser.add_option("-o", "--output-file", action="store", type="string", dest="output_file",
                      help="The destination of output file")


    return parser.parse_args()

def building_structure_map(track_set, album_set, artist_set, genre_set, feature_mapping):
    global num_feature
    with open(data_directory + track_data) as f:
        reader = csv.reader(f, delimiter=DELIM)
        for row in reader:
            track_set.add(row[0])
    with open(data_directory + album_data) as f:
        reader = csv.reader(f, delimiter=DELIM)
        for row in reader:
            album_set.add(row[0])
            feature_mapping[row[0]] = num_feature
            num_feature += 1
    with open(data_directory + artist_data) as f:
        reader = csv.reader(f, delimiter=DELIM)
        for row in reader:
            artist_set.add(row[0])
            feature_mapping[row[0]] = num_feature
            num_feature += 1
    with open(data_directory + genre_data) as f:
        reader = csv.reader(f, delimiter=DELIM)
        for row in reader:
            genre_set.add(row[0])
            feature_mapping[row[0]] = num_feature
            num_feature += 1

def key_appending(item_id, album_set, artist_set, genre_set):
    if(item_id in album_set):
        return 'album' + item_id
    elif (item_id in artist_set):
        return 'artist' + item_id
    elif (item_id in genre_set):
        return 'genre' + item_id
    else:
        return None

def album_key_appending(item_id, album_set):
    if(item_id in album_set):
        return item_id
    return 'album'+'None'

def artist_key_appending(item_id, artist_set):
    if(item_id in artist_set):
        return item_id
    return 'artist'+'None'

def genre_key_appending(item_id, genre_set):
    if(item_id in genre_set):
        return item_id
    return 'genre'+'None'

def building_track_item(album_set, artist_set, genre_set, feature_mapping, out_file):
    global num_feature
    with open(data_directory + track_data) as f:
        reader = csv.reader(f, delimiter=DELIM)
        for row in reader:
            out_str = row[0] + TAB_DELIM + str(feature_mapping['track']) + ':1'
            album_key = album_key_appending(row[1], album_set)
            artist_key = artist_key_appending(row[2], artist_set)
            if album_key not in feature_mapping:
                feature_mapping[album_key] = num_feature
                num_feature+=1
            if artist_key not in feature_mapping:
                feature_mapping[artist_key] = num_feature
                num_feature+=1
            out_str = out_str + TAB_DELIM + str(feature_mapping[album_key]) + ':1' + TAB_DELIM + str(feature_mapping[artist_key]) + ':1'
            for i in range(3, len(row)): #genre
                genre_key = genre_key_appending(row[i], genre_set)
                if genre_key not in feature_mapping:
                    feature_mapping[appended_key] = num_feature
                    num_feature+=1
                out_str = out_str + TAB_DELIM + str(feature_mapping[genre_key]) + ':1'
            out_file.write(out_str + '\n')

def building_album_item(album_set, artist_set, genre_set, feature_mapping, out_file):
    with open(data_directory + album_data) as f:
        reader = csv.reader(f, delimiter=DELIM)
        for row in reader:
            out_str = row[0] + TAB_DELIM + str(feature_mapping['album']) + ':1'
            album_key = row[0];
            artist_key = artist_key_appending(row[1], artist_set)
            if artist_key not in feature_mapping:
                feature_mapping[artist_key] = num_feature
                num_feature+=1
            out_str = out_str + TAB_DELIM + str(feature_mapping[album_key]) + ':1' + TAB_DELIM + str(feature_mapping[artist_key]) + ':1'
            for i in range(2,len(row)):
                genre_key = genre_key_appending(row[i], genre_set)
                if genre_key not in feature_mapping:
                    feature_mapping[appended_key] = num_feature
                    num_feature+=1
                out_str = out_str + TAB_DELIM + str(feature_mapping[genre_key]) + ':1'
            out_file.write(out_str + '\n')

def building_artist_item(artist_set, genre_set, feature_mapping, out_file):
    with open(data_directory + artist_data) as f:
        reader = csv.reader(f, delimiter=DELIM)
        for row in reader:
            out_str = row[0] + TAB_DELIM + str(feature_mapping['artist']) + ':1'
            artist_key = row[0]
            out_str = out_str + TAB_DELIM + str(feature_mapping[artist_key]) + ':1'
            out_file.write(out_str + '\n')

def building_genre_item(genre_set, feature_mapping, out_file):
    with open(data_directory + genre_data) as f:
        reader = csv.reader(f, delimiter=DELIM)
        for row in reader:
            out_str = row[0] + TAB_DELIM + str(feature_mapping['genre']) + ':1'
            genre_key = row[0]
            out_str = out_str + TAB_DELIM + str(feature_mapping[genre_key]) + ':1'
            out_file.write(out_str + '\n')




def building_items(track_set, album_set, artist_set, genre_set, feature_mapping, output_file):
    out_file = open(output_file,'w')
    building_track_item(album_set, artist_set, genre_set, feature_mapping, out_file)
    building_album_item(album_set, artist_set, genre_set, feature_mapping, out_file)
    building_artist_item(artist_set, genre_set, feature_mapping, out_file)
    building_genre_item(genre_set, feature_mapping, out_file)
    out_file.close()

if __name__ == "__main__":
    (options, args) = parse_command_line()
    track_set = Set()
    album_set = Set()
    artist_set= Set()
    genre_set = Set()
    feature_mapping = {}
    feature_mapping['track'] = num_feature
    num_feature+=1
    feature_mapping['album'] = num_feature
    num_feature+=1
    feature_mapping['artist'] = num_feature
    num_feature+=1
    feature_mapping['genre'] = num_feature
    num_feature+=1

    building_structure_map(track_set, album_set, artist_set, genre_set,feature_mapping)
    building_items(track_set,album_set,artist_set,genre_set,feature_mapping, options.output_file)
