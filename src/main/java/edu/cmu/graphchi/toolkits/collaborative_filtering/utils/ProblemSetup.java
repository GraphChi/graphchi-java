package edu.cmu.graphchi.toolkits.collaborative_filtering.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.GnuParser;

import edu.cmu.graphchi.ChiLogger;

/**
 * This class represents the setup of a particular algorithm to be run.
 * Every recommender system algorithm need some common parameters like the 
 * location of training data file, test data file, number of shards, min and
 * max values, etc.
 * Each algorithm might also have some custom arguments like regularization or
 * step. 
 * Currently, this class wraps the Apache CLI and parses all the command line
 * arguments and sets its member variables. The values of custom parameters are read
 * by the corresponding algorithms ModelParameter class.
 * @author mayank
 */

public class ProblemSetup {
	
		private static final DateFormat DF = new SimpleDateFormat("MM/dd/yyyy_HH:mm:ss");

		public String runId;	//The identifier by which this particular run will be identified. 
        
		public double minval = Double.MIN_VALUE;	//The minimum value of rating
		public double maxval = Double.MAX_VALUE;	//The maximum value of rating
        
        //TODO: Standardize this so that user / item features can be taken as input as well.
        public String training;					//Training file (currrently in Matrix market Format)
        public String validation;				//Validation File
        public String test;						//Test File
        public String userFeatures;				//File containing user features.
        public String itemFeatures;				//File containing item features.
        
        public int nShards;						//Number of shards to run with GraphChi
        public int quiet;							//Debug information?
        public String paramJson;					//JSON String representing the parameters
        public String paramFile;					//File containing parameters as JSON
        public String outputLoc;				//The url of output dir/S3 bucket etc. to save the model parameters.  
        
        public ProblemSetup(String[] args) {
        	this.parse_command_line_arguments(args);
        }
        
        //TODO: Use Apache CLI or some other package which has a better command line interface.
        public void parse_command_line_arguments(String [] args){
        	Options options = new Options();
        	options.addOption("runId", true, "The id to be used to identify the current run. Default is" +
        			" of the following format <algo>_<timestamp>");
        	options.addOption("training", true, "The training file in Matrix Market format");
        	options.addOption("validation", true, "The validation file in Matrix Market format");
        	options.addOption("test", true, "The test file in Matrix Market format");
        	
        	options.addOption("userFeatures", true, "The file containing user features");
        	options.addOption("itemFeatures", true, "The file containing item features");
        	
        	options.addOption("minval", true, "The minimum value of predictions");
        	options.addOption("maxval", true, "The maximum value of predictions");
        	
        	options.addOption("nshards", true, "Number of shards for GraphChi");
        	options.addOption("paramJson", true, "JSON String representing the parameters");
        	options.addOption("paramFile", true, "Number of shards for GraphChi");
        	options.addOption("quiet", true, "boolean flag indicating whether to display log information");
        	options.addOption("outputLoc", true, "The location (dir / url) to which the output should be serialized");
        	HelpFormatter help = new HelpFormatter();
        	
    		try{
    			CommandLineParser parser = new GnuParser();
            	CommandLine cmd = parser.parse(options, args);
    			
            	this.runId = cmd.getOptionValue("runId");
            	
    			this.training = cmd.getOptionValue("training");
    			if(this.training == null) {
    				System.out.println("Missing required argument --training \n");
    				help.printHelp("<algorithm> <options>", options);
    				System.exit(1);
    			}
    			this.validation = cmd.getOptionValue("validation");
    			this.test = cmd.getOptionValue("test");
    		
    			this.userFeatures = cmd.getOptionValue("userFeatures");
    			this.itemFeatures = cmd.getOptionValue("itemFeatures");
    			
    			this.minval = Integer.parseInt(cmd.getOptionValue("minval", "" + Integer.MIN_VALUE));
    			this.maxval = Integer.parseInt(cmd.getOptionValue("maxval", "" + Integer.MAX_VALUE));
    			
    			this.nShards = Integer.parseInt(cmd.getOptionValue("nshards", "1"));
    			
    			//Parameters.
    			this.paramJson = cmd.getOptionValue("paramJson");
    			this.paramFile = cmd.getOptionValue("paramFile");
    			
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

		public String getRunId(String algo) {
			if(this.runId == null) {
				return algo + "_" + DF.format(new Date());
			} else {
				return this.runId;
			}
		}
      
}
