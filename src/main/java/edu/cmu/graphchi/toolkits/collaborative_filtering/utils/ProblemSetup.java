package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.nio.file.Paths;
import java.util.Vector;
import java.util.logging.Level;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.pig.parser.InvalidCommandException;
import org.eclipse.jdt.core.compiler.InvalidInputException;

import edu.cmu.graphchi.ChiLogger;

/**
 * This class represents the setup of a particular algorithm to be run.
 * Every recommender system algorithm need some common parameters like the 
 * location of training data file, test data file, number of shards, min and
 * max values, etc.
 * Currently, this class wraps the Apache CLI and parses all the command line
 * arguments and sets its member variables. The values of custom parameters are read
 * by the corresponding algorithms ModelParameter class.
 * @author mayank
 */

public class ProblemSetup {
	
        public int nShards;						//Number of shards to run with GraphChi
        public int quiet;							//Debug information?
        public String dataMetadataFile;
        public String paramFile;		//File containing parameters as JSON
        public String outputLoc;		//The url of output dir/S3 bucket etc. to save the model parameters.  
        public String scratchDir;		//The scratchDir to be used by graphchi for sharding		
        public String graphChiJar;		//The location of graphchi jar.
        
        private ProblemSetup() {
        	
        }
        
        public ProblemSetup(String[] args) {
        	this.parse_command_line_arguments(args);
        }
        
        public void parse_command_line_arguments(String [] args){
        	Options options = new Options();
        	options.addOption("nshards", true, "Number of shards for GraphChi");
        	options.addOption("dataMetadataFile", true, "Metadata about input data. Should be " +
        			"json file with complete description of input data.");
        	options.addOption("paramFile", true, "File containing parameters for different models");
        	options.addOption("quiet", true, "boolean flag indicating whether to display log information");
        	options.addOption("outputLoc", true, "The location (dir / url) to which the output should be serialized");
        	options.addOption("scratchDir", true, "The path where graphchi will create its temporary files");
        	options.addOption("graphChiJar", true, "The path to the graphchi jar");
        	HelpFormatter help = new HelpFormatter();
        	
    		try{
    			CommandLineParser parser = new ExtendedGnuParser(true);
            	CommandLine cmd = parser.parse(options, args);
    			
    			this.nShards = Integer.parseInt(cmd.getOptionValue("nshards", "1"));
    			
    			//Parameters.
    			this.paramFile = verifyRequiredValue(cmd.getOptionValue("paramFile"));
    			
    			this.dataMetadataFile = verifyRequiredValue(cmd.getOptionValue("dataMetadataFile"));
    			
    			this.quiet = Integer.parseInt(cmd.getOptionValue("quiet", "0"));
    			this.outputLoc = cmd.getOptionValue("outputLoc", "./");
    			
    			this.scratchDir = cmd.getOptionValue("scratchDir", "/tmp/abc");
    			
    			this.graphChiJar = cmd.getOptionValue("graphChiJar", "graphchi-java-0.2-jar-with-dependencies.jar");
    			
    		} catch (Exception ex){
    			System.out.println("Failed to parse command line parameters: " + ex.toString());
    			help.printHelp("java -cp <graphchi_jar> " +
                        "edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.AggregateRecommender <options>", options);
    			
    			System.exit(1);
    		}
    	
    		if (this.quiet > 0){
    			ChiLogger.getLogger("engine").setLevel(Level.SEVERE);
    		    ChiLogger.getLogger("memoryshard").setLevel(Level.SEVERE);
    		}
        }
        
        private String verifyRequiredValue(String optionsVal) throws InvalidInputException {
            if(optionsVal == null) {
                throw new InvalidInputException("The command entered is wrong");
            } else {
                return optionsVal;
            }
        }
        
        @Override
        public ProblemSetup clone() {
        	ProblemSetup cloned = new ProblemSetup();
        	cloned.nShards = this.nShards;
        	cloned.paramFile = this.paramFile;
        	cloned.dataMetadataFile = this.dataMetadataFile;
        	cloned.graphChiJar = this.graphChiJar;
        	cloned.quiet = this.quiet;
        	cloned.outputLoc = this.outputLoc;
        	cloned.scratchDir = this.scratchDir;
        	
        	return cloned;
        }
        
        @Override
        public String toString() {
            Vector<CharSequence> vargs = new Vector<CharSequence>(100);
            
            vargs.add("--nshards=" + nShards);
            vargs.add("--graphChiJar=" + graphChiJar);
            vargs.add("--paramFile=" + paramFile);
            vargs.add("--dataMetadataFile=" + this.dataMetadataFile);
            vargs.add("--scratchDir=" + this.scratchDir);
            vargs.add("--outputLoc=" + this.outputLoc);
            vargs.add("--quiet=" + this.quiet);
            
            StringBuilder command = new StringBuilder(); 
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
              }
            
            return command.toString();
        }
        
        //For testing purposes
        public static void main(String[] args) {
        	//String[] args2 = "--nshards=4     --paramFile=\"/home/mayank/Capstone/als_desc.json\"   --dataMetadataFile=\"/media/sda5/Capstone/Movielens/ml-100k/hdfs_ml-100k_desc.json\"".split(" ");
        	/*for(int i = 0; i < args.length; i++) {
        		System.out.println(i + ": " + args[i]);
        	}*/
            
        	ProblemSetup setup = new ProblemSetup(args);
        	System.out.println(setup.toString());
        	System.out.println(setup.clone());
        }
        
}
