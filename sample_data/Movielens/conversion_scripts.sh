python movielens_userfeatures.py /media/Data1/Capstone/Movielens/ml-100k/working_dir/u.user;
python movielens_item_features.py /media/Data1/Capstone/Movielens/ml-100k/working_dir/u.item;
python convert_to_mm.py -g /media/Data1/Capstone/Movielens/ml-100k/working_dir/u.data -e 100000 -u '{"file_name":"/media/Data1/Capstone/Movielens/ml-100k/working_dir/u.user.processed", "num":943}' -i '{"file_name":"/media/Data1/Capstone/Movielens/ml-100k/working_dir/u.item.processed", "num":1682}';
