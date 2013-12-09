#!/bin/bash
sudo mkdir /opt/yarn
sudo chown yarn:hadoop /opt/yarn -R
sudo cp ./config /opt/yarn
sudo mkdir -p /var/data/hadoop/hdfs/nn; sudo mkdir -p /var/data/hadoop/hdfs/snn; sudo mkdir -p /var/data/hadoop/hdfs/dn
sudo chown yarn:hadoop /var/data/hadoop/hdfs -R
su yarn -c 'AWS_CONFIG_FILE=/opt/yarn/config aws s3 cp s3://apache-yarn/hadoop-2.2.0.tar.gz /opt/yarn/;
cd /opt/yarn && tar -xvf /opt/yarn/hadoop-2.2.0.tar.gz;
mkdir /opt/yarn/hadoop-2.2.0/tmp;
AWS_CONFIG_FILE=/opt/yarn/config aws s3 cp s3://apache-yarn/configs/core-site.xml /opt/yarn/hadoop-2.2.0/etc/hadoop/core-site.xml
AWS_CONFIG_FILE=/opt/yarn/config aws s3 cp s3://apache-yarn/configs/hdfs-site.xml /opt/yarn/hadoop-2.2.0/etc/hadoop/hdfs-site.xml
AWS_CONFIG_FILE=/opt/yarn/config aws s3 cp s3://apache-yarn/configs/yarn-site.xml /opt/yarn/hadoop-2.2.0/etc/hadoop/yarn-site.xml
AWS_CONFIG_FILE=/opt/yarn/config aws s3 cp s3://apache-yarn/configs/yarn-env.sh  /opt/yarn/hadoop-2.2.0/etc/hadoop/yarn-env.sh
AWS_CONFIG_FILE=/opt/yarn/config aws s3 cp s3://apache-yarn/.bashrc  /home/yarn/.bashrc'
