
-- Test script

REGISTER /Users/akyrola/Projects/graphchi-java/target/graphchi-java-research-jar-with-dependencies.jar


A = load '/Users/akyrola/graphs/testdata' using PigStorage('\t') as (src:int, dst:int);

DESCRIBE A;

STORE A INTO '/tmp/graph';

C = load '/tmp/graph' using com.twitter.pers.pig.PagerankPigUDF(4);



STORE C into '/tmp/testout2';