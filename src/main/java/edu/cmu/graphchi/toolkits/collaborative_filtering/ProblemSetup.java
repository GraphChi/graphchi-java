package edu.cmu.graphchi.toolkits.collaborative_filtering;

import java.util.logging.Level;

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

		long M,N,L;
        int D = 10;	//Hidden features - Move to some other place?
        double minval = Double.MIN_VALUE;
        double maxval = Double.MAX_VALUE;
        String training;
        String validation;
        String test;
        int nShards;
        int quiet;
        String paramJson;
        
        public ProblemSetup(String[] args) {
        	this.parse_command_line_arguments(args);
        }
        
        //TODO: Use Apache CLI or some other package which has a better command line interface.
        public void parse_command_line_arguments(String [] args){
    		try{
	    		for (int i=0; i< args.length; i++){
	    			if (args[i].startsWith("--training="))
	    				this.training = args[i].substring(11, args[i].length());
	    			else if (args[i].startsWith("--validation="))
	    				this.validation = args[i].substring(13, args[i].length());
	    			else if (args[i].startsWith("--test="))
	    				this.test = args[i].substring(7,args[i].length());
	    			else if (args[i].startsWith("--D="))
	    				this.D = Integer.parseInt(args[i].substring(4,args[i].length()));
	    			else if (args[i].startsWith("--minval="))
	    				this.minval = Integer.parseInt(args[i].substring(9,args[i].length()));
	    			else if (args[i].startsWith("--maxval="))
	    				this.maxval = Integer.parseInt(args[i].substring(9,args[i].length()));
	    			else if (args[i].startsWith("--nshards="))
	    				this.nShards = Integer.parseInt(args[i].substring(10,args[i].length()));
	    			else if (args[i].startsWith("--quiet="))
	    				this.quiet = Integer.parseInt(args[i].substring(8,args[i].length()));
	    		}
    		} catch (Exception ex){
    			System.out.println("Failed to parse command line parameters: " + ex.toString());
    			System.exit(1);
    		}
    	
    		if (this.quiet > 0){
    			ChiLogger.getLogger("engine").setLevel(Level.SEVERE);
    		    ChiLogger.getLogger("memoryshard").setLevel(Level.SEVERE);
    		}
        }
      
}
