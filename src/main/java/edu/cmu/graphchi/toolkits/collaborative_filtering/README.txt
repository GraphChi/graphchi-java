Following file describes how to run the various algorithms in GraphChi on a local computer as well as on a YARN cluster.

====== RUNNING ON LOCAL MACHINE ======
The following are the steps to run on a local machine. Movielens 100k data provided in the sample_data directory is used
as a running example.

Step 1:
Build the jar. From the home directory of the repository run following command. 
mvn assembly:assembly -DdescriptorId=jar-with-dependencies
A jar named "graphchi-java-0.2-jar-with-dependencies.jar" will be created in the target directory.

Step 2:
Define the Algorithms JSON file. This file contains the description of all the recommender algorithms 
that are to be run for the dataset. A sample of this file is present in sample_data/all_desc.json. It 
requires to set various parameters like regularization and step size to be run. To understand the details 
of these parameters, please see the source code which has a reference to the relevant papers and equations.

Step 3:
Define the Dataset Description file. This file contains the location of the datasets and some metadata
about them. One such example for 100k Movielens dataset is provided in sample_data/file_ml-100k_desc.json
file.

Step 4: 
Run the program using a command as follows:
java -Xmx1024m -cp ./target/graphchi-java-0.2-jar-with-dependencies.jar edu.cmu.graphchi.toolkits.collaborave_filtering.algorithms.AggregateRecommender --nshards=4 --paramFile="sample_data/all_desc.json" --dataMetadataFile="sample_data/file_ml-100k_desc.json" --scratchDir=/tmp/abc --outputLoc=/tmp/

Step 5:
Testing the trained model. Once all algorithms have converged, the serialized model files will be saved in the
location specified by "outputLoc" commandline parameter. These model files can then be used to test the 
accuracy of the model parameters by running on test data. Following setup is required for testing:
   Step a: Similar to input DatasetDescription JSON file, there should be a test dataset description file.
   To create this, replace the "ratingLoc" to test file location instead of training file location and replace
   numRatings to number of ratings in the testing data. One such example of a file for Movielens dataset is 
   present in sample_data/file_test_ml-100k_desc.json
   Step b: Create a test Description JSON file, which lists all the model file locations and the list of testing 
   metrics to use. An example of such file is present in sample_data/all_tests.json
   Step c: Run Command:
   java -Xmx1024m -cp ./target/graphchi-java-0.2-jar-with-dependencies.jar edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.TestPredictions --nshards=4 --testDescFile="sample_data/all_tests.json" --dataMetadataFile="sample_data/file_test_ml-100k_desc.json"


===== FORMAT OF THE FILES ======
Note that input data provided to the toolkit can present in any format, but would require implementing an InputDataReader 
class to parse the data and provide it to GraphChi. See the "InputDataReader" interface for more descriptions. Currently,
data from delimited files can be read. These files can be present in local file system, HDFS or some public URL (like S3).
Following describes the files required and their format:

1. Ratings File (Required):
This file consists of all edges (ratings given by users to items). It should be in Matrix Market format as follows

%%MatrixMarket matrix coordinate real general
% <COMMENTS>
<NumUsers> <NumItems> <NumRatings>
<UserId>    <ItemId>    <Rating>    <Edge Features Str> 

Here, UserId and ItemId, both should start from 1. Rating is a double.
<Edge Feature Str> contains a list of <featureId>:<featureVal> tuples in following format: 
"<int>:<double>  <int>:<double>  <int>:<double> ..."

Feature Id is an integer starting from 1 and featureVal is double representing a value of the feature. 
For example, featureId 1 may represent "time since rated" and featureVal of "15" may represent 15 days.

2. User Features File (Optional). 
This file consists of user features, each line is of the following format:
<UserId>    <User Feature Str>

<User Feature Str> contains a list of <featureId>:<featureVal> tuples in following format:
"<int>:<double>  <int>:<double>  <int>:<double> ..."

FeatureId is an integer starting from 1 representing a particular user feature and featureVal is the 
corresponding value of the feature. For example, 1:1 may represent "Gender Male":"Yes" as one of the
boolean features for the user and "4:20" may represent "Age":20 as a feature for the user.

3. Item Feature File. (Optional).
This is a file with the same format as User Feature File, but containing item features instead.

========= RUNNING ON YARN =========
See the YARN_README.txt for details. This would first require setting up a YARN cluster and then running the
algorithms on the cluster


