/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
    
package edu.cmu.graphchi.toolkits.collaborative_filtering.yarn;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import com.google.common.annotations.VisibleForTesting;

import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.RecommenderAlgorithm;
import edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.RecommenderFactory;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.DataSetDescription;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.IO;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.ProblemSetup;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.RecommenderPool;
import edu.cmu.graphchi.toolkits.collaborative_filtering.utils.RecommenderScheduler;

/**
 * 
 * Naive GraphChi's Application master based on the Example provided in 
 * YARN package.
 * 
 * <p>
 * The ApplicationMaster is started on a container by the
 * <code>ResourceManager</code>'s launcher. The first thing that the
 * <code>ApplicationMaster</code> needs to do is to connect and register itself
 * with the <code>ResourceManager</code>. The registration sets up information
 * within the <code>ResourceManager</code> regarding what host:port the
 * ApplicationMaster is listening on to provide any form of functionality to a
 * client as well as a tracking url that a client can use to keep track of
 * status/job history if needed. However, in the current implementation, trackingurl
 * and appMasterHost:appMasterRpcPort are not supported.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> needs to send a heartbeat to the
 * <code>ResourceManager</code> at regular intervals to inform the
 * <code>ResourceManager</code> that it is up and alive. The
 * {@link ApplicationMasterProtocol#allocate} to the <code>ResourceManager</code> from the
 * <code>ApplicationMaster</code> acts as a heartbeat.
 * 
 * <p>
 * For the actual handling of the job, the <code>ApplicationMaster</code> has to
 * request the <code>ResourceManager</code> via {@link AllocateRequest} for the
 * required no. of containers using {@link ResourceRequest} with the necessary
 * resource specifications such as node location, computational
 * (memory/disk/cpu) resource requirements. The <code>ResourceManager</code>
 * responds with an {@link AllocateResponse} that informs the
 * <code>ApplicationMaster</code> of the set of newly allocated containers,
 * completed containers as well as current state of available resources.
 * </p>
 * 
 * <p>
 * For each allocated container, the <code>ApplicationMaster</code> can then set
 * up the necessary launch context via {@link ContainerLaunchContext} to specify
 * the allocated container id, local resources required by the executable, the
 * environment to be setup for the executable, commands to execute, etc. and
 * submit a {@link StartContainerRequest} to the {@link ContainerManagementProtocol} to
 * launch and execute the defined commands on the given allocated container.
 * </p>
 * 
 * <p>
 * The <code>ApplicationMaster</code> can monitor the launched container by
 * either querying the <code>ResourceManager</code> using
 * {@link ApplicationMasterProtocol#allocate} to get updates on completed containers or via
 * the {@link ContainerManagementProtocol} by querying for the status of the allocated
 * container's {@link ContainerId}.
 *
 * <p>
 * After the job has been completed, the <code>ApplicationMaster</code> has to
 * send a {@link FinishApplicationMasterRequest} to the
 * <code>ResourceManager</code> to inform it that the
 * <code>ApplicationMaster</code> has been completed.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class ApplicationMaster {

    private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);
  
    private static final String AGG_REC_MAIN_CLASS = "edu.cmu.graphchi.toolkits.collaborative_filtering.algorithms.AggregateRecommender";

    // Configuration
    private Configuration conf;
    
    private AMRMClient<ContainerRequest> amRMClient;
    
    // Handle to communicate with the Node Manager
    private NMClientAsync nmClientAsync;
    // Listen to process the response from the Node Manager
    private NMCallbackHandler containerListener;
      
    // Application Attempt Id ( combination of attemptId and fail count )
    private ApplicationAttemptId appAttemptID;
    
    ProblemSetup setup;
    
    // Hostname of the container
    private String appMasterHostname = "";
    // Port on which the app master listens for status updates from clients
    private int appMasterRpcPort = -1;
    // Tracking url to which app master publishes info for clients to monitor
    private String appMasterTrackingUrl = "";
    
    // App Master configuration
    // No. of containers to run shell command on
    private int numTotalContainers;
      
    // Priority of the request
    private int requestPriority=1;
    
    // Counter for completed containers ( complete denotes successful or failed )
    private AtomicInteger numCompletedContainers = new AtomicInteger();
    // Count of failed containers
    private AtomicInteger numFailedContainers = new AtomicInteger();
     
    // Args to be passed to the shell command
    private Map<String, String> shellEnv = new HashMap<String, String>();
    
    private volatile boolean success;
    
    private ByteBuffer allTokens;
    
    // Launch threads
    private List<Thread> launchThreads = new ArrayList<Thread>();
    
    List<RecommenderAlgorithm> recommenders;
    
    /**
     * @param args Command line args
     */
    public static void main(String[] args) {
        boolean result = false;
        LOG.info("Initializing ApplicationMaster1");
        try {
            LOG.info("Initializing ApplicationMaster");
            ApplicationMaster appMaster = new ApplicationMaster();
            System.out.println("Initializing ApplicationMaster");
            boolean doRun = appMaster.init(args);
            if (!doRun) {
                System.exit(0);
            }
            result = appMaster.run();
        } catch (Throwable t) {
            LOG.fatal("Error running ApplicationMaster", t);
            System.exit(1);
        }
        if (result) {
            LOG.info("Application Master completed successfully. exiting");
            System.exit(0);
        } else {
            LOG.info("Application Master failed. exiting");
            System.exit(2);
        }
    }
    
    
    public ApplicationMaster() {
        // Set up the configuration
        conf = new YarnConfiguration();
    }
    
    /**
     * Parse command line options
     *
     * @param args Command line args
     * @return Whether init successful and run should be invoked
     * @throws ParseException
     * @throws IOException
    */
    public boolean init(String[] args) throws ParseException, IOException {
        LOG.info("Inside Init");
        this.setup = new ProblemSetup(args);
          
        Map<String, String> envs = System.getenv();
          
        ContainerId containerId = ConverterUtils.toContainerId(envs
                  .get(Environment.CONTAINER_ID.name()));
        appAttemptID = containerId.getApplicationAttemptId();
        
        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV)) {
              throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV
                  + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HOST.name())) {
            throw new RuntimeException(Environment.NM_HOST.name()
                  + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name())) {
            throw new RuntimeException(Environment.NM_HTTP_PORT
                 + " not set in the environment");
        }
        if (!envs.containsKey(Environment.NM_PORT.name())) {
            throw new RuntimeException(Environment.NM_PORT.name()
                  + " not set in the environment");
        }
        LOG.info("Application master for app" + ", appId="
              + appAttemptID.getApplicationId().getId() + ", clustertimestamp="
              + appAttemptID.getApplicationId().getClusterTimestamp()
              + ", attemptId=" + appAttemptID.getAttemptId());
          
          //TODO: Based on GraphChi estimates, set appropriate containerMemory and number of containers.
            DataSetDescription dataDesc = new DataSetDescription();
            dataDesc.loadFromJsonFile(setup.dataMetadataFile);
            this.recommenders = RecommenderFactory.buildRecommenders(dataDesc, setup.paramFile, null);
         
            
            return true;
        }
    
    /**
     * Main run function for the application master
     *
     * @throws YarnException
     * @throws IOException
     */
    @SuppressWarnings({ "unchecked" })
    public boolean run() throws YarnException, IOException {
        LOG.info("Starting ApplicationMaster");
    
        Credentials credentials =
            UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        // Now remove the AM->RM token so that containers cannot access it.
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        
        amRMClient = AMRMClient.createAMRMClient();
        amRMClient.init(conf);
        amRMClient.start();
    
        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();
    
        // Register self with ResourceManager
        // This will start heartbeating to the RM
        appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient
            .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                appMasterTrackingUrl);
    
        int maxMem = 500;
        LOG.info("Max mem capabililty of resources in this cluster " + maxMem);
        
        int numTotalNodes = amRMClient.getClusterNodeCount();
        
        int numNodes = computeMaxNodesRequired(maxMem);
        
        //Ideally, we should ask RM for containers based on the status of the cluster (how many nodes with how
        // much memory is available). However, I do not know how to get "cluster state" in Apache YARN (like 
        // google's Omega). 
        // Hence, current logic is: Based on maxMem (assuming heterogenous?), just ask for all possible containers.
        // Once some containers are allocated, split the recommenders to these machines and cancel any further 
        // allocated containers.
        for (int i = 0; i < numNodes; ++i) {
            ContainerRequest containerAsk = setupContainerAskForRM(maxMem, requestPriority);
            amRMClient.addContainerRequest(containerAsk);
        }
    
        float progress = 0;
        boolean pending = true;
        this.numTotalContainers = -1;
        int count = 0;
        while (numCompletedContainers.get() != numTotalContainers) {
            try {
                count++;
                if(pending && count > 20) {
                    LOG.info("Allocation of container taking too long. Is there something wrong with container request?");
                    break;
                }
          
                Thread.sleep(200);
          
                AllocateResponse allocResp = amRMClient.allocate(progress);
                List<Container> newContainers = allocResp.getAllocatedContainers();
                List<ContainerStatus> completedContainers = allocResp.getCompletedContainersStatuses();
        
                if(newContainers.size() > 0) {
                    LOG.info("Allocated " + newContainers.size() + " new containers");
                    if(pending) {
                        //If there are pending recommenders, use the container to run the jobs.
                        RecommenderScheduler sched = new RecommenderScheduler(newContainers.size(), maxMem, recommenders);
                        List<RecommenderPool> recPools = sched.splitIntoRecPools();
                        for(int i = 0; i < newContainers.size(); i++) {
                            startContainer(newContainers.get(i), recPools.get(i));
                        }
                        //Assigned all the GraphChi jobs
                        pending = false;
                        numTotalContainers = newContainers.size();
                    } else {
                        //Release these containers as no longer required.
                        for(Container c : newContainers) {
                            LOG.info("Releasing extra " + newContainers.size() + " new containers");
                            amRMClient.releaseAssignedContainer(c.getId());
                        }
                    }
                }
              
                onContainersCompleted(completedContainers);
       
                } catch (InterruptedException ex) {}
        }
        finish();
        
        return success;
    }
    
    private int computeMaxNodesRequired(int maxMem) {
        int totalMemRequirement = 0;
        
        for(RecommenderAlgorithm rec : this.recommenders) {
            totalMemRequirement += rec.getEstimatedMemoryUsage();
        }
        
        return (int)Math.ceil((double)totalMemRequirement/maxMem);
    }
    
    @VisibleForTesting
    NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }
      
    /**
     * Setup the request that will be sent to the RM for the container ask.
     *
     * @return the setup ResourceRequest to be sent to RM
     */
    private ContainerRequest setupContainerAskForRM(int memory, int priority) {
        // setup requirements for hosts
        // using * as any host will do for the distributed shell app
        // set the priority for the request
        Priority pri = Records.newRecord(Priority.class);
        // TODO - what is the range for priority? how to decide?
        pri.setPriority(priority);
    
        // Set up resource type requirements
        // For now, only memory is supported so we set memory requirements
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(memory);
    
        ContainerRequest request = new ContainerRequest(capability, null, null, pri);
        LOG.info("Requested container ask: " + request.toString());
        return request;
    }
      
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
        LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
        for (ContainerStatus containerStatus : completedContainers) {
            LOG.info("Got container status for containerID="
                    + containerStatus.getContainerId() + ", state="
                    + containerStatus.getState() + ", exitStatus="
                    + containerStatus.getExitStatus() + ", diagnostics="
                    + containerStatus.getDiagnostics());
    
            // non complete containers should not be here
            assert (containerStatus.getState() == ContainerState.COMPLETE);
    
            // increment counters for completed/failed containers
            int exitStatus = containerStatus.getExitStatus();
            if (0 != exitStatus) {
                // container failed
                if (ContainerExitStatus.ABORTED != exitStatus) {
                    // shell script failed
                    // counts as completed
                    numCompletedContainers.incrementAndGet();
                    numFailedContainers.incrementAndGet();
                } else {
                    // container was killed by framework, possibly preempted
                    // we should re-try as the container was lost for some reason
                    //TODO: Add retry
                    numCompletedContainers.incrementAndGet();
                    numFailedContainers.incrementAndGet();
             
                    // we do not need to release the container as it would be done
                    // by the RM
                }
            } else {
                //nothing to do
                // container completed successfully
                numCompletedContainers.incrementAndGet();
                LOG.info("Container completed successfully." + ", containerId="
                        + containerStatus.getContainerId());
            }
        }
    }
    
    public void startContainer(Container c, RecommenderPool pool) {
        LOG.info("Launching Rec Pool command on a new container."
            + ", containerId=" + c.getId()
            + ", containerNode=" + c.getNodeId().getHost()
            + ":" + c.getNodeId().getPort()
            + ", containerNodeURI=" + c.getNodeHttpAddress()
            + ", containerResourceMemory"
            + c.getResource().getMemory());
    
        LaunchContainerRunnable runnableLaunchContainer =
            new LaunchContainerRunnable(c, containerListener, pool);
        Thread launchThread = new Thread(runnableLaunchContainer);
    
        // launch and start the container on a separate thread to keep
        // the main thread unblocked
        // as all containers may not be allocated at one go.
        launchThreads.add(launchThread);
        launchThread.start();
    }
    
    private void finish() {
        // Join all launched threads
        // needed for when we time out
        // and we need to release containers
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(10000);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage());
                e.printStackTrace();
            }
        }
    
        // When the application completes, it should stop all running containers
        LOG.info("Application completed. Stopping running containers");
        nmClientAsync.stop();
    
        // When the application completes, it should send a finish application
        // signal to the RM
        LOG.info("Application completed. Signalling finish to RM");
    
        FinalApplicationStatus appStatus;
        String appMessage = null;
        success = true;
        if (numFailedContainers.get() == 0 && 
                numCompletedContainers.get() == numTotalContainers) {
            appStatus = FinalApplicationStatus.SUCCEEDED;
        } else {
            appStatus = FinalApplicationStatus.FAILED;
            appMessage = "Diagnostics." + ", total=" + numTotalContainers
                    + ", completed=" + numCompletedContainers.get() + ", failed="
                    + numFailedContainers.get();
            success = false;
        }
        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            LOG.error("Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("Failed to unregister application", e);
        }
        
        amRMClient.stop();
    }
    
    
    @VisibleForTesting
    static class NMCallbackHandler
        implements NMClientAsync.CallbackHandler {
    
        private ConcurrentMap<ContainerId, Container> containers =
            new ConcurrentHashMap<ContainerId, Container>();
        private final ApplicationMaster applicationMaster;
    
        public NMCallbackHandler(ApplicationMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }
    
        public void addContainer(ContainerId containerId, Container container) {
            containers.putIfAbsent(containerId, container);
        }
    
        @Override
        public void onContainerStopped(ContainerId containerId) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to stop Container " + containerId);
            }
            containers.remove(containerId);
        }
    
        @Override
        public void onContainerStatusReceived(ContainerId containerId,
            ContainerStatus containerStatus) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Container Status: id=" + containerId + ", status=" +
                        containerStatus);
            }
        }
    
        @Override
        public void onContainerStarted(ContainerId containerId,
                Map<String, ByteBuffer> allServiceResponse) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Succeeded to start Container " + containerId);
            }
            Container container = containers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
        }
    
        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to start Container " + containerId);
            containers.remove(containerId);
            applicationMaster.numCompletedContainers.incrementAndGet();
            applicationMaster.numFailedContainers.incrementAndGet();
        }
    
        @Override
        public void onGetContainerStatusError(
            ContainerId containerId, Throwable t) {
            LOG.error("Failed to query the status of Container " + containerId);
        }
    
        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("Failed to stop Container " + containerId);
            containers.remove(containerId);
        }
    }
    
    /**
     * Thread to connect to the {@link ContainerManagementProtocol} and launch the container
     * that will execute the shell command.
    */
    private class LaunchContainerRunnable implements Runnable {
        // Allocated container
        Container container;
    
        NMCallbackHandler containerListener;
    
        //The list of recommender algorithms to be run in this container 
        RecommenderPool pool;
    
        /**
         * @param lcontainer Allocated container
         * @param containerListener Callback handler of the container
         */
        public LaunchContainerRunnable(
                Container lcontainer, NMCallbackHandler containerListener, RecommenderPool pool) {
            this.container = lcontainer;
            this.containerListener = containerListener;
            this.pool = pool;
        }
    
        @Override
        /**
         * Connects to CM, sets up container launch context 
         * for shell command and eventually dispatches the container 
         * start request to the CM. 
         */
        public void run() {
            LOG.info("Setting up container launch container for containerid="
                    + container.getId());
            ContainerLaunchContext ctx = Records
                    .newRecord(ContainerLaunchContext.class);
    
            // Set the environment
            //TODO: Make this generic
            //shellEnv.put("HADOOP_HOME", "/home/mayank/Softwares/hadoop-2.2/hadoop-2.2.0");
            //shellEnv.put("HADOOP_CONF_DIR", "/home/mayank/Softwares/hadoop-2.2/hadoop-2.2.0/etc/hadoop");
            shellEnv.put("HADOOP_HOME", System.getenv().get("HADOOP_HOME"));
            shellEnv.put("HADOOP_CONF_DIR", System.getenv().get("HADOOP_CONF_DIR"));
            
            ctx.setEnvironment(shellEnv);
            
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
            
            try {
                //Create a new parameter file for the given pool
                String newParamFile = "params_" + container.getId() + ".json";
                pool.createParamJsonFile(newParamFile);
                
                //Update the problem setup object to point to this new parameter file
                ProblemSetup probSetup = setup.clone();
                probSetup.paramFile = newParamFile;
                
                probSetup = ApplicationMaster.localizeResources(probSetup, localResources);
    
                ctx.setLocalResources(localResources);
                
                String command = createExecutableCommand(container, probSetup);
                
                List<String> commands = new ArrayList<String>();
                commands.add(command.toString());
                ctx.setCommands(commands);
                // Set up tokens for the container too. Today, for normal shell commands,
                // the container in distribute-shell doesn't need any tokens. We are
                // populating them mainly for NodeManagers to be able to download any
                // files in the distributed file-system.
                ctx.setTokens(allTokens.duplicate());
        
                containerListener.addContainer(container.getId(), container);
                nmClientAsync.startContainerAsync(container, ctx);
            } catch (Exception e) {
                e.printStackTrace();
                //release container?
                numCompletedContainers.incrementAndGet();
                numFailedContainers.incrementAndGet();
            }            
        }
        
        public String createExecutableCommand(Container container, ProblemSetup probSetup) {
            // Set the necessary command to execute on the allocated container
            Vector<CharSequence> vargs = new Vector<CharSequence>(5);
    
            // Set executable command
            vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    
            //Heap memory
            vargs.add("-Xmx" + container.getResource().getMemory() + "m");
            
            //Jar path
            vargs.add("-cp " + (new File(probSetup.graphChiJar).getName()));
            //Main class name
            vargs.add(AGG_REC_MAIN_CLASS);
    
            // Set args for the shell command if any
            //TODO: Number of shards might vary based on our estimate of memory usage.
            vargs.add(probSetup.toString());
            
            // Add log redirect params
            vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
            vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
    
            // Get final commmand
            StringBuilder command = new StringBuilder();
            for (CharSequence str : vargs) {
                command.append(str).append(" ");
            }
            
            return command.toString();
        }
    
    }
    
    public static ProblemSetup localizeResources(ProblemSetup probSetup, Map<String, LocalResource> localResources) 
            throws Exception {
        ProblemSetup dupSetup = probSetup.clone();
        
        //Setup the local resources required to run a graphchi program.
        String remoteParamPath, remoteDataDescPath, remoteJavaJarPath;
          
        LocalResource resource = addLocalResource(probSetup.graphChiJar);
        remoteJavaJarPath = (new File(probSetup.graphChiJar).getName()); 
        localResources.put(remoteJavaJarPath, resource);
        dupSetup.graphChiJar = remoteJavaJarPath;
      
        resource = addLocalResource(probSetup.paramFile);
        remoteParamPath = (new File(probSetup.paramFile).getName());
        localResources.put(remoteParamPath, resource);
        dupSetup.paramFile = remoteParamPath;
      
        resource = addLocalResource(probSetup.dataMetadataFile);
        remoteDataDescPath = (new File(probSetup.dataMetadataFile)).getName();
        localResources.put(remoteDataDescPath, resource);
        dupSetup.dataMetadataFile = remoteDataDescPath;
      
        return dupSetup;
    }
        
    public static LocalResource addLocalResource(String filePath) throws Exception {
        File file = new File(filePath);
        String fullFilePath = file.getAbsolutePath();
        
        FileSystem fs = FileSystem.get(IO.getConf());
        Path src = new Path(fullFilePath);
        String pathSuffix = "local-tmp/" + file.getName();        
        Path dst = new Path(fs.getHomeDirectory(), pathSuffix);
        fs.copyFromLocalFile(false, true, src, dst);
        FileStatus destStatus = fs.getFileStatus(dst);
        LocalResource resource = Records.newRecord(LocalResource.class);
        
        resource.setType(LocalResourceType.FILE);
        resource.setVisibility(LocalResourceVisibility.APPLICATION);       
        resource.setResource(ConverterUtils.getYarnUrlFromPath(dst)); 
        resource.setTimestamp(destStatus.getModificationTime());
        resource.setSize(destStatus.getLen());
            
        return resource;
    }
      
}
