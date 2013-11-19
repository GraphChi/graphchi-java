package edu.cmu.graphchi.toolkits.collaborative_filtering.yarn;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ExtendedGnuParser;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;

/**
 * Client for Distributed Shell application submission to YARN.
 * 
 * <p> The distributed shell client allows an application master to be launched that in turn would run 
 * the provided shell command on a set of containers. </p>
 * 
 * <p>This client is meant to act as an example on how to write yarn-based applications. </p>
 * 
 * <p> To submit an application, a client first needs to connect to the <code>ResourceManager</code> 
 * aka ApplicationsManager or ASM via the {@link ApplicationClientProtocol}. The {@link ApplicationClientProtocol} 
 * provides a way for the client to get access to cluster information and to request for a
 * new {@link ApplicationId}. <p>
 * 
 * <p> For the actual job submission, the client first has to create an {@link ApplicationSubmissionContext}. 
 * The {@link ApplicationSubmissionContext} defines the application details such as {@link ApplicationId} 
 * and application name, the priority assigned to the application and the queue
 * to which this application needs to be assigned. In addition to this, the {@link ApplicationSubmissionContext}
 * also defines the {@link ContainerLaunchContext} which describes the <code>Container</code> with which 
 * the {@link ApplicationMaster} is launched. </p>
 * 
 * <p> The {@link ContainerLaunchContext} in this scenario defines the resources to be allocated for the 
 * {@link ApplicationMaster}'s container, the local resources (jars, configuration files) to be made available 
 * and the environment to be set for the {@link ApplicationMaster} and the commands to be executed to run the 
 * {@link ApplicationMaster}. <p>
 * 
 * <p> Using the {@link ApplicationSubmissionContext}, the client submits the application to the 
 * <code>ResourceManager</code> and then monitors the application by requesting the <code>ResourceManager</code> 
 * for an {@link ApplicationReport} at regular time intervals. In case of the application taking too long, the client 
 * kills the application by submitting a {@link KillApplicationRequest} to the <code>ResourceManager</code>. </p>
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {
    
    private static final Log LOG = LogFactory.getLog(Client.class);

    // Configuration
    private Configuration conf;
    private YarnClient yarnClient;
    // Application master specific info to register a new Application with RM/ASM
    private String appName = "";
    // App master priority
    private int amPriority = 0;
    // Queue for App master
    private String amQueue = "";
    // Amt. of memory resource to request for to run the App Master
    private int amMemory = 100; 

    // Application master jar file
    private String appMasterJar = ""; 
    // Main class to invoke application master
    private final String appMasterMainClass;

    // Args to be passed to the shell command
    private String shellArgs = "";

    // Amt of memory to request for container in which shell script will be executed
    private int containerMemory = 10; 

    // log4j.properties file 
    // if available, add to local resources and set into classpath 
    private String log4jPropFile = "";	

    // Start time for client
    private final long clientStartTime = System.currentTimeMillis();
    // Timeout threshold for client. Kill app after time interval expires.
    private long clientTimeout = 600000;

    // Debug flag
    boolean debugFlag = false;	

    // Command line options
    private Options opts;
  
    ProblemSetup setup;

    /**
     * @param args Command line arguments 
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            Client client = new Client();
            LOG.info("Initializing Client");
            try {
                boolean doRun = client.init(args);
                if (!doRun) {
                    System.exit(0);
                }
            } catch (IllegalArgumentException e) {
                System.err.println(e.getLocalizedMessage());
                client.printUsage();
                System.exit(-1);
            }
            result = client.run();
        } catch (Throwable t) {
            LOG.fatal("Error running CLient", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application completed successfully");
            System.exit(0);			
        } 
        LOG.error("Application failed to complete successfully");
        System.exit(2);
    }

    /**
     */
    public Client(Configuration conf) throws Exception  {
        this("edu.cmu.graphchi.toolkits.collaborative_filtering.yarn.ApplicationMaster",
                conf);
    }

    Client(String appMasterMainClass, Configuration conf) {
        this.conf = conf;
        this.appMasterMainClass = appMasterMainClass;
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        opts = new Options();
        opts.addOption("appname", true, "Application Name. Default value - DistributedShell");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("timeout", true, "Application timeout in milliseconds");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption("graphChiJar", true, "Jar file containing the application master");
        opts.addOption("log_properties", true, "log4j.properties file");
        opts.addOption("debug", false, "Dump out debug information");
        opts.addOption("help", false, "Print usage");
    }

    /**
     */
    public Client() throws Exception  {
        this(new YarnConfiguration());
    }

    /**
     * Helper function to print out usage
     */
    private void printUsage() {
        new HelpFormatter().printHelp("Client", opts);
    }

    /**
     * Parse command line options
     * @param args Parsed command line options 
     * @return Whether the init was successful to run the client
     * @throws ParseException
     */
    public boolean init(String[] args) throws ParseException {

        CommandLine cliParser = new ExtendedGnuParser(true).parse(opts, args);

        if (args.length == 0) {
            throw new IllegalArgumentException("No args specified for client to initialize");
        }		

        if (cliParser.hasOption("help")) {
            printUsage();
            return false;
        }

        if (cliParser.hasOption("debug")) {
            debugFlag = true;
        }

        appName = cliParser.getOptionValue("appname", "GraphChiProgram");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority", "0"));
        amQueue = cliParser.getOptionValue("queue", "default");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory", "100"));		

        if (amMemory < 0) {
            throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
                    + " Specified memory=" + amMemory);
        }   

        if (!cliParser.hasOption("graphChiJar")) {
            throw new IllegalArgumentException("No jar file specified for application master");
        }		

        appMasterJar = cliParser.getOptionValue("graphChiJar");

        if (cliParser.hasOption("args")) {
            shellArgs = cliParser.getOptionValue("args");
            System.out.println(shellArgs);
        }
    
        containerMemory = Integer.parseInt(cliParser.getOptionValue("container_memory", "10"));

        clientTimeout = Integer.parseInt(cliParser.getOptionValue("timeout", "600000"));

        log4jPropFile = cliParser.getOptionValue("log_properties", "");

        this.setup = new ProblemSetup(args);
    
        return true;
    }

    /**
     * Main run function for the client
     * @return true if application completed successfully
     * @throws IOException
     * @throws YarnException
     */
    public boolean run() throws IOException, YarnException {

        LOG.info("Running Client");
        yarnClient.start();
    
        YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
        LOG.info("Got Cluster metric info from ASM" 
            + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());
    
        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(
            NodeState.RUNNING);
        LOG.info("Got Cluster node info from ASM");
        for (NodeReport node : clusterNodeReports) {
            LOG.info("Got node report from ASM for"
                    + ", nodeId=" + node.getNodeId() 
                    + ", nodeAddress" + node.getHttpAddress()
                    + ", nodeRackName" + node.getRackName()
                    + ", nodeNumContainers" + node.getNumContainers());
        }

        QueueInfo queueInfo = yarnClient.getQueueInfo(this.amQueue);
        LOG.info("Queue info"
            + ", queueName=" + queueInfo.getQueueName()
            + ", queueCurrentCapacity=" + queueInfo.getCurrentCapacity()
            + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
            + ", queueApplicationCount=" + queueInfo.getApplications().size()
            + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());		

        List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
        for (QueueUserACLInfo aclInfo : listAclInfo) {
          for (QueueACL userAcl : aclInfo.getUserAcls()) {
            LOG.info("User ACL Info for Queue"
                + ", queueName=" + aclInfo.getQueueName()			
                + ", userAcl=" + userAcl.name());
          }
        }		

        // Get a new application id
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        // TODO get min/max resource capabilities from RM and change memory ask if needed
        // If we do not have min/max, we may not be able to correctly request 
        // the required resources from the RM for the app master
        // Memory ask has to be a multiple of min and less than max. 
        // Dump out information about cluster capability as seen by the resource manager
        int maxMem = appResponse.getMaximumResourceCapability().getMemory();
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
    
        // A resource ask cannot exceed the max. 
        if (amMemory > maxMem) {
          LOG.info("AM memory specified above max threshold of cluster. Using max value."
              + ", specified=" + amMemory
              + ", max=" + maxMem);
          amMemory = maxMem;
        }				

        // set the application name
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        appContext.setApplicationName(appName);
    
        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
    
        // set local resources for the application master
        // local files or archives as needed
        // In this scenario, the jar file for the application master is part of the local resources			
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    
    
        try {
        	ApplicationMaster.addLocalResource(appMasterJar);
        	//Localize other resources (graphchi jar, paramFile, dataMetadata file)
        	this.setup = ApplicationMaster.localizeResources(this.setup, localResources);
        } catch (Exception e) {
        	e.printStackTrace();
        	LOG.error("Error while localizing");
        }
    
        // Set local resource info into app master container launch context
        amContainer.setLocalResources(localResources);
    
        // Set the necessary security tokens as needed
        //amContainer.setContainerTokens(containerToken);
    
        // Set the env variables to be setup in the env where the application master will be run
        LOG.info("Set the environment for the application master");
        Map<String, String> env = new HashMap<String, String>();
        env.put("HADOOP_HOME", System.getenv().get("HADOOP_HOME"));
    	env.put("HADOOP_CONF_DIR", System.getenv().get("HADOOP_CONF_DIR"));
    
        // Add AppMaster.jar location to classpath 		
        // At some point we should not be required to add 
        // the hadoop specific classpaths to the env. 
        // It should be provided out of the box. 
        // For now setting all required classpaths including
        // the classpath to "." for the application jar
        StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$())
          .append(File.pathSeparatorChar).append("./*");
        for (String c : conf.getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
          classPathEnv.append(File.pathSeparatorChar);
          classPathEnv.append(c.trim());
        }
        classPathEnv.append(File.pathSeparatorChar).append("./log4j.properties");
    
        // add the runtime classpath needed for tests to work
        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
          classPathEnv.append(':');
          classPathEnv.append(System.getProperty("java.class.path"));
        }
    
        env.put("CLASSPATH", classPathEnv.toString());
    
        amContainer.setEnvironment(env);
        
        // Set the necessary command to execute the application master 
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);
    
        // Set java executable command 
        LOG.info("Setting up app master command");
        vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx" + amMemory + "m");
        // Set class name 
        vargs.add(appMasterMainClass);
    
        vargs.add(this.setup.toString());
        
        if (debugFlag) {
          vargs.add("--debug");
        }
        
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");
    
        // Get final commmand
        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
          command.append(str).append(" ");
        }
    
        LOG.info("Completed setting up app master command " + command.toString());	   
        List<String> commands = new ArrayList<String>();
        commands.add(command.toString());		
        amContainer.setCommands(commands);
    
        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);
        appContext.setResource(capability);
    
        // Service data is a binary blob that can be passed to the application
        // Not needed in this scenario
        // amContainer.setServiceData(serviceData);
    
        // Setup security tokens
        if (UserGroupInformation.isSecurityEnabled()) {
          Credentials credentials = new Credentials();
          String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
          if (tokenRenewer == null || tokenRenewer.length() == 0) {
            throw new IOException(
              "Can't get Master Kerberos principal for the RM to use as renewer");
          }
    
          DataOutputBuffer dob = new DataOutputBuffer();
          credentials.writeTokenStorageToStream(dob);
          ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
          amContainer.setTokens(fsTokens);
        }
    
        appContext.setAMContainerSpec(amContainer);
    
        // Set the priority for the application master
        Priority pri = Records.newRecord(Priority.class);
        // TODO - what is the range for priority? how to decide? 
        pri.setPriority(amPriority);
        appContext.setPriority(pri);
    
        // Set the queue to which this application is to be submitted in the RM
        appContext.setQueue(amQueue);
    
        // Submit the application to the applications manager
        // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
        // Ignore the response as either a valid response object is returned on success 
        // or an exception thrown to denote some form of a failure
        LOG.info("Submitting application to ASM");
    
        yarnClient.submitApplication(appContext);
    
        // TODO
        // Try submitting the same request again
        // app submission failure?
    
        // Monitor the application
        return monitorApplication(appId);

    }

    /**
     * Monitor the submitted application for completion. 
     * Kill application if time expires. 
     * @param appId Application Id of application to be monitored
     * @return true if application completed successfully
     * @throws YarnException
     * @throws IOException
     */
    private boolean monitorApplication(ApplicationId appId)
            throws YarnException, IOException {
        while (true) {

            // Check app status every 1 second.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.debug("Thread sleep in monitoring loop interrupted");
            }
    
            // Get application report for the appId we are interested in 
            ApplicationReport report = yarnClient.getApplicationReport(appId);

            LOG.info("Got application report from ASM for"
                    + ", appId=" + appId.getId()
                    + ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
                    LOG.info("Application has completed successfully. Breaking monitoring loop");
                    return true;        
                } else {
                    LOG.info("Application did finished unsuccessfully."
                          + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                          + ". Breaking monitoring loop");
                    return false;
                }			  
            } else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
                LOG.info("Application did not finish."
                        + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                        + ". Breaking monitoring loop");
                return false;
            }			

            if (System.currentTimeMillis() > (clientStartTime + clientTimeout)) {
                LOG.info("Reached client specified timeout for application. Killing application");
                forceKillApplication(appId);
                return false;				
            }
        }			
    }

    /**
     * Kill a submitted application by sending a call to the ASM
     * @param appId Application Id to be killed. 
     * @throws YarnException
     * @throws IOException
     */
    private void forceKillApplication(ApplicationId appId)
            throws YarnException, IOException {
        // TODO clarify whether multiple jobs with the same app id can be submitted and be running at 
        // the same time. 
        // If yes, can we kill a particular attempt only?

        // Response can be ignored as it is non-null on success or 
        // throws an exception in case of failures
        yarnClient.killApplication(appId);	
    }
}