import boto.ec2
from fabric.api import run, sudo, env, local
import time
from optparse import OptionParser
from os.path import basename, splitext

HADOOP_HOME = "/opt/yarn/hadoop-2.2.0"
HADOOP_CONF_DIR = "/opt/yarn/hadoop-2.2.0/etc/hadoop"

YARN_PW = "hadooppw"

# Makes sure that a newly launched instance / stopped instance is back up and running.
def start_aws_instance(ins):
    status = ins.update()
    if status == 'stopped':
        ins.start()
    
    while status != 'running':
        time.sleep(10)
        print ".",
        status = ins.update()

    host = ins.public_dns_name
    
    if status == "running":
        retry = True
        while retry:
            env.user = "ec2-user"
            env.host_string = host
            env.key_filename = key_filename
            try:
                retry = False
                run("hostname")
            except:
                print "Sleeping 10 seconds ..."
                retry = True
                time.sleep(10)

    print "Instance " + host + " is now running and can be sshed to" 
    
# Runs various commands and does requires installation to setup the aws instance properly
def configure_aws_instance(host, user="ec2-user"):
    env.user = user
    env.host_string = host
    env.key_filename = key_filename
    
    create_aws_config_file(user, host)
    
    #Download all the deployment scripts into the machine.
    run("AWS_CONFIG_FILE=./config aws s3 cp --recursive s3://apache-yarn/scripts .")
    sudo("chmod 777 *.sh");
    sudo("./configure_aws_instance.sh");
    

def deploy_yarn(slave_instances, master_instance, host, user="ec2-user"):
    env.user = user
    env.host_string = host
    env.key_filename = key_filename
    
    create_aws_config_file(user, host)

    run("AWS_CONFIG_FILE=./config aws s3 cp --recursive s3://apache-yarn/scripts .")
    sudo("chmod 777 *.sh");
    run("./deploy_yarn.sh")
    
    #Formats HDFS if this is master
    if master_instance.public_dns_name == host:
        run("su yarn -c './bin/hdfs namenode -format'")
    

def start_yarn_services(host, user="yarn", password=YARN_PW):
    env.user = user
    env.host_string = host
    env.password = password

    run(HADOOP_HOME + "/sbin/start-dfs.sh")
    run(HADOOP_HOME + "/sbin/start-yarn.sh")
    #run(HADOOP_HOME + "/sbin/hadoop-daemon.sh start namenode")
    #run(HADOOP_HOME + "/sbin/hadoop-daemon.sh start datanode")
    #run(HADOOP_HOME + "/sbin/yarn-daemon.sh start resourcemanager")
    #run(HADOOP_HOME + "/sbin/yarn-daemon.sh start nodemanager")
    
    
def stop_yarn_services(host, user="yarn", password=YARN_PW):
    env.user = user
    env.host_string = host
    env.password = password
    
    run(HADOOP_HOME + "/sbin/stop-dfs.sh")
    run(HADOOP_HOME + "/sbin/stop-yarn.sh")
    #run(HADOOP_HOME + "/sbin/hadoop-daemon.sh stop namenode")
    #run(HADOOP_HOME + "/sbin/hadoop-daemon.sh stop datanode")
    #run(HADOOP_HOME + "/sbin/yarn-daemon.sh stop resourcemanager")
    #run(HADOOP_HOME + "/sbin/yarn-daemon.sh stop nodemanager")

    
def stop_aws_instance(host):
    pass

def create_hosts_file(slave_instances, master_instance, user="ec2-user"):
    env.user = user
    env.key_filename = key_filename
    
    with open("./hosts", "w") as host_file:
        host_file.write(master_instance.private_ip_address + " " + master_instance.private_dns_name + " master\n")
        #host_file.write(master_instance.ip_address + " " + master_instance.public_dns_name + " master\n")
        i = 1
        for ins in slave_instances:
            host_file.write(ins.private_ip_address + " " + ins.private_dns_name + " slave" + str(i) + "\n")
            #host_file.write(ins.ip_address + " " + ins.public_dns_name + " slave" + str(i) + "\n")
            i = i+1
    
    env.password = YARN_PW
    #Copy this hosts file to all hosts
    for ins in slave_instances:
        env.host_string = ins.public_dns_name
        local("scp -i " + key_filename + " hosts " + user + "@" + ins.public_dns_name + ":/tmp")
        sudo("cp /tmp/hosts" + " /etc/hosts")
    
    #Copy the hosts file to master and create a etc/hadoop/slaves file in master
    env.host_string = master_instance.public_dns_name
    local("scp -i " + key_filename + " hosts " + user + "@" + master_instance.public_dns_name + ":/tmp")
    sudo("cp /tmp/hosts" + " /etc/hosts")
    
    with open("./slaves", "w") as slaves_file:
        for ins in slave_instances:
            slaves_file.write(ins.public_dns_name + "\n")
    env.host_string = master_instance.public_dns_name
    local("scp slaves yarn@" + master_instance.public_dns_name + ":" 
        + HADOOP_CONF_DIR + "/slaves")
    
    
# Creates the AWS config file on the remote host which contains the secret key and access ids.
# This enables accessing S3 buckets
# TODO: Check if this is needed when the bucket has been made public  
def create_aws_config_file(user, hostname, remote_dir="."):
        #Create config to enable accessing various scripts / other things needed for deployment. 
    with open("config", "w") as file:
        file.write("[default]\n")
        file.write("aws_access_key_id = " + aws_access_key_id + "\n")
        file.write("aws_secret_access_key = " + aws_secret_access_key + "\n")
        file.write("region = " + region + "\n")
    
    local("scp -i " + key_filename + " config " + user + "@" + hostname + ":"+remote_dir)

def verify_required_option(arg, var_name):
    if arg is None:
        parser.error(var_name + " is required")
    else:
        return arg
        
if __name__ == "__main__":
    parser = OptionParser(usage="%prog [options]")
    parser.add_option("-i", "--instance_ids_str", help="Commma separated list of EC2 Instance Ids in the cluster." + 
        " They should have already been launched", dest="instance_ids_str")
    parser.add_option("--master_id", help="Instance Id of the master", dest="master_id")
    parser.add_option("--deploy_to_ids", help="Comma separated list of AMI ids to which YARN has to be deployed" + 
        " (WARN: HDFS  will be formatted)", dest="deploy_ids_str")
    parser.add_option("--aws_access_key_id", help="Access key for AWS", dest="aws_access_key_id")
    parser.add_option("--aws_secret_access_key", help="AWS secret id", dest="aws_secret_access_key")
    parser.add_option("--region", help="Region for AWS", dest="region", default="us-east-1")
    parser.add_option("--key_file", help="Key file for AWS", dest="key_file")
    parser.add_option("--run_slave_on_master", help="Boolean flag to denote whether slave process " + 
    "should be run on master", dest="run_slave_on_master", action="store_false")
    
    (options, args) = parser.parse_args()
    
    key_filename = verify_required_option(options.key_file, "--key_file")
    key_name = splitext(basename(key_filename))[0]
    aws_access_key_id = verify_required_option(options.aws_access_key_id, "--aws_access_key_id")
    aws_secret_access_key = verify_required_option(options.aws_secret_access_key, "--aws_secret_access_key")
    region = verify_required_option(options.region, "--region")

    # Connect to the AWS region
    conn = boto.ec2.connect_to_region(region, aws_access_key_id=aws_access_key_id, 
                                      aws_secret_access_key=aws_secret_access_key)
    
    all_instance_ids = verify_required_option(options.instance_ids_str, "instance_ids_str").split(',')
    master_instance_id = verify_required_option(options.master_id, "master_id")
    all_instances = conn.get_only_instances(all_instance_ids)
    master_instance = conn.get_only_instances(options.master_id)[0]
    
    if options.deploy_ids_str is not None:
        deploy_instances = conn.get_only_instances(options.deploy_ids_str.split(','))
    else:
        deploy_instances = []
    
    slave_instances = []
    for ins in all_instances:
        if options.run_slave_on_master or ins.id != master_instance.id:
            slave_instances.append(ins)
    
    #Start all the instances in the cluster
    for ins in all_instances:
        status = ins.update()
        host = ins.public_dns_name
        if status == 'stopped':
            print "Restarting host " + host
            start_aws_instance(ins)

    for ins in deploy_instances:
        print "Configuring host " + host
        configure_aws_instance(host=host)

        print "Deloying yarn on AWS instance " + host
        deploy_yarn(slave_instances, master_instance, host)        

    create_hosts_file(slave_instances, master_instance, "ec2-user")
    
    #setup passwordless ssh from master to hosts
    env.host_string = master_instance.public_dns_name
    env.user = "yarn"
    env.password = YARN_PW
    for ins in slave_instances:
        run("/opt/yarn/set_passwordless_ssh.sh " + ins.public_dns_name + " yarn")
    
    stop_yarn_services(master_instance.public_dns_name, user="yarn")
    start_yarn_services(master_instance.public_dns_name, user="yarn")
    
#Launch AWS instance. Returns a newly launched instance
# Not being used anywhere in the code currently. 
def launch_aws_instance(details):
    reservation = conn.run_instances(
        'ami-5755743e',
        key_name=key_name,
        instance_type='m1.small',
        security_groups=['default'])
    
    ins = reservation.instances[0]
    host = ins.public_dns_name
    
    print "Starting newly launched instance " + host
    start_aws_instance(ins)
    
    return [ins]