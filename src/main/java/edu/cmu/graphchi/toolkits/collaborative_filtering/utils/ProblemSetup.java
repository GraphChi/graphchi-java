package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.util.logging.Level;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

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
        public String paramFile;					//File containing parameters as JSON
        public String outputLoc;				//The url of output dir/S3 bucket etc. to save the model parameters.  
        
        public ProblemSetup(String[] args) {
        	this.parse_command_line_arguments(args);
        }
        
        //TODO: Use Apache CLI or some other package which has a better command line interface.
        public void parse_command_line_arguments(String [] args){
        	Options options = new Options();
        	options.addOption("nshards", true, "Number of shards for GraphChi");
        	options.addOption("dataMetadataFile", true, "Metadata about input data. Should be " +
        			"json file with complete description of input data.");
        	options.addOption("paramFile", true, "File containing parameters for different models");
        	options.addOption("quiet", true, "boolean flag indicating whether to display log information");
        	options.addOption("outputLoc", true, "The location (dir / url) to which the output should be serialized");
        	HelpFormatter help = new HelpFormatter();
        	
    		try{
    			CommandLineParser parser = new GnuParser();
            	CommandLine cmd = parser.parse(options, args);
    			
    			
    			this.nShards = Integer.parseInt(cmd.getOptionValue("nshards", "1"));
    			
    			//Parameters.
    			this.paramFile = cmd.getOptionValue("paramFile");
    			
    			this.dataMetadataFile = cmd.getOptionValue("dataMetadataFile");
    			
    			this.quiet = Integer.parseInt(cmd.getOptionValue("quiet", "1"));
    			this.outputLoc = cmd.getOptionValue("outputLocation", "./");
    			
    		} catch (Exception ex){
    			System.out.println("Failed to parse command line parameters: " + ex.toString());
    			help.printHelp("<algorithm> <options>", options);
    			
    			System.exit(1);
    		}
    	
    		if (this.quiet > 0){
    			ChiLogger.getLogger("engine").setLevel(Level.SEVERE);
    		    ChiLogger.getLogger("memoryshard").setLevel(Level.SEVERE);
    		}
        }
}
