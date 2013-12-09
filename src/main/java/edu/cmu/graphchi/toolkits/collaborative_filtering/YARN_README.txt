This document list the steps required in setting up a YARN cluster on AWS. Setting up a YARN cluster involves
decent number of steps. To make this process faster, it is a good idea to configure 1 instance 
and create an AMS AMI image. This AWS AMI image can then be used for further launches of YARN cluster.
Once YARN has been individually deployed to all instances, there is a helper script "deploy_yarn.py"
which can be used to start a YARN cluster.

1. Launch / Start and AWS instance (have tested it using standard AWS AMI) from EC2 management console.

2. Configure the AWS instance
    We need to create certain users and groups to make AWS instance ready to be used for deploying YARN. Login to the host using the Key pair as follows:
ssh -i <Key_File> ec2-user@<public_dns_name>

a. Make sure that /  file has following entry as the corresponding values. It allows users to ssh to EC2 instance instead of using the key file
---------------------------------------
PasswordAuthentication yes    
---------------------------------------

b. Restart the ssh daemon
sudo /etc/init.d/sshd restart


b. Add user and group
----------------------------------------
sudo groupadd hadoop
sudo useradd yarn
sudo passwd yarn
----------------------------------------
sudo /etc/init.d/sshd restart


3. Deploy YARN
a. Make all essential directories for HDFS and yarn deployment. (on all hosts)
sudo mkdir /opt/yarn; sudo chown yarn:hadoop /opt/yarn -R
sudo mkdir -p /var/data/hadoop/hdfs/nn; sudo mkdir -p /var/data/hadoop/hdfs/snn; sudo mkdir -p /var/data/hadoop/hdfs/dn
sudo chown yarn:hadoop /var/data/hadoop/hdfs -R

b. Edit /etc/hosts file on all hosts to contain all the slaves and master. For master make sure that there is the string “master” listed as one of the aliases corresponding to the host you can want to make as master.
For  example:
<private ip address>  <private dns name> master
<private ip address>  <private dns name> slave1
<private ip address>  <private dns name> slave2

c. Download YARN tarball and untar it (either form the official website or from aws bucket)
su yarn -c 'AWS_CONFIG_FILE=/opt/yarn/config aws s3 cp s3://apache-yarn/hadoop-2.2.0.tar.gz /opt/yarn/;
cd /opt/yarn
su yarn -c 'tar -xvf /opt/yarn/hadoop-2.2.0.tar.gz;'

d. Make following changes in configuration files for YARN.

etc/hadoop/yarn-site.xml
NOTE: The value of "yarn.scheduler.maximum-allocation-mb" and "yarn.nodemanager.resource.memory-mb" can be
changed based on the AWS instance. Following website describes configs available: 
http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-common/yarn-default.xml
------------------------
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
    <property>
        <name>yarn.resourcemanager.resource-tracker.address</name>
        <value>master:8025</value>
     </property>
     <property>
        <name>yarn.resourcemanager.scheduler.address</name>
        <value>master:8030</value>
    </property>
    <property>
        <name>yarn.resourcemanager.address</name>
        <value>master:8040</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-mb</name>
        <value>1536</value>
    </property>
    <property>
        <name>yarn.scheduler.minimum-allocation-mb</name>
        <value>512</value>
    </property>
    <property>
        <name>yarn.nodemanager.resource.memory-mb</name>
        <value>1536</value>
    </property>
    <property>
        <name>yarn.scheduler.increment-allocation-mb</name>
        <value>256</value>
    </property>
    <property>
        <name>yarn.scheduler.maximum-allocation-vcores</name>
        <value>1</value>
        <description>Virtual Cores</description>
    </property>
    <property>
        <name>yarn.nodemanager.container-monitor.interval-ms</name>
        <value>10000</value>
    </property>
</configuration>
------------------------------------

core-site.xml
----------------------------------------
<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://master:9000</value>
    </property>
    <property>
        <name>hadoop.http.staticuser.user</name>
        <value>yarn</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/opt/yarn/hadoop-2.2.0/tmp</value>
    </property>
    <property>
        <name>fs.file.impl</name>
        <value>org.apache.hadoop.fs.LocalFileSystem</value>
    </property>
    <property>
        <name>fs.hdfs.impl</name>
        <value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
        <description>The FileSystem for hdfs: uris.</description>
    </property>
</configuration>
--------------------------------------

etc/hadoop/hdfs-site.xml
-------------------------------------
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>2</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:/var/data/hadoop/hdfs/nn</value>
    </property>
    <property>
        <name>fs.checkpoint.dir</name>
        <value>file:/var/data/hadoop/hdfs/snn</value>
    </property>
    <property>
        <name>fs.checkpoint.edits.dir</name>
        <value>file:/var/data/hadoop/hdfs/snn</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:/var/data/hadoop/hdfs/dn</value>
    </property>
</configuration>
------------------------------------------------

etc/hadoop/yarn-env.sh
------------------------------------------------
Add following lines:
export JAVA_HOME=/usr/lib/jvm/jre
export HADOOP_HOME=/opt/yarn/hadoop-2.2.0
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
----------------------------------------------

On master: add following file:
etc/hadoop/slaves
<private_ip_address of slave1>
<private_ip_address of slave2>
<private_ip_address of slave3>

e. Format name node.
su yarn -c './bin/hdfs namenode -format'

4. Start Services
./bin/start-dfs.sh
./bin/start-yarn.sh

5. Verify that the following processes are up and running. NameNode and ResourceManager on Master node. 
DataNode and NodeManager on Slave nodes (by running "ps -ef | grep yarn" command).

6. Submit a YARN client job.
./bin/yarn jar /opt/yarn/graphchi-java-0.2-jar-with-dependencies.jar edu.cmu.graphchi.toolkits.collaborative_filtering.yarn.Client --graphChiJar=/opt/yarn/graphchi-java-0.2-jar-with-dependencies.jar --nshards=4 --paramFile=<param_file> --dataMetadataFile=<datasetDescFile> --scratchDir=<scratch_dir> --outputLoc=<output_location>

Note that the commandline alogrithms to the YARN client program contains all options passed to local run of graphchi program (see README.txt for more details) and an extra required option is "graphChiJar=<jar_location>"


The log files for ResourceManager, NodeManager, NameNode and DataNode are present in /opt/yarn/hadoop-2.2.0/logs directory
The log files specific to the application is present in /opt/yarn/hadoop-2.2.0/userLogs directory on each host.
Going though log on each host is cumbersome even in a small cluster. Following recent blog entry describes a way
to aggregate YARN logs: http://hortonworks.com/blog/simplifying-user-logs-management-and-access-in-yarn/

