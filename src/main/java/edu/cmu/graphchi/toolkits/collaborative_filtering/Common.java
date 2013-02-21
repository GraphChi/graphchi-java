package edu.cmu.graphchi.toolkits.collaborative_filtering;

public class Common {

	public static void parse_command_line_arguments(String [] args){
		
		try{
		for (int i=0; i< args.length; i++){
			if (args[i].startsWith("--training="))
				ProblemSetup.training = args[i].substring(9, args[i].length());
			else if (args[i].startsWith("--validation="))
				ProblemSetup.validation = args[i].substring(12, args[i].length());
			else if (args[i].startsWith("--test="))
				ProblemSetup.test = args[i].substring(7,args[i].length());
			else if (args[i].startsWith("--D="))
				ProblemSetup.D = Integer.parseInt(args[i].substring(4,args[i].length()));
			else if (args[i].startsWith("--minval="))
				ProblemSetup.minval = Integer.parseInt(args[i].substring(9,args[i].length()));
			else if (args[i].startsWith("--maxval="))
				ProblemSetup.maxval = Integer.parseInt(args[i].substring(9,args[i].length()));
			else if (args[i].startsWith("--nshards="))
				ProblemSetup.nShards = Integer.parseInt(args[i].substring(10,args[i].length()));
		}
		} catch (Exception ex){
			ProblemSetup.logger.info("Failed to parse command line parameters: " + ex.toString());
			System.exit(1);
		}
		
	}
}
