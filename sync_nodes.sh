#!/bin/bash


for node_ip in "$@"
do
   echo "Syncing to $node_ip ..."
   #ssh root@$node_ip 'rm -fr /usr/local/backup/*'
   ssh root@$node_ip 'mkdir -p /usr/local/backup/agent' 
   rsync -P /root/.pip/pip.conf root@$node_ip:/root/.pip/pip.conf
   rsync -P /etc/apt/sources.list root@$node_ip:/etc/apt/sources.list
   rsync -rLP  ./agent/* root@$node_ip:/usr/local/backup/agent/
   #ssh root@$node_ip 'pip install --find-links=https://pypi.python.org/simple/ -r /usr/local/backup/agent/requirements.txt'
   ssh root@$node_ip 'unlink /etc/init/csobackuprestore.conf'
   ssh root@$node_ip 'cp /usr/local/backup/agent/csobackuprestore.conf /etc/init/; mkdir -p /backup'
   ssh root@$node_ip 'service csobackuprestore restart'
   ssh root@$node_ip 'sed -i "/^path.repo.*$/d" /etc/elasticsearch/elasticsearch.yml'
   ssh root@$node_ip 'echo "path.repo: [\"/backup\"]" >> /etc/elasticsearch/elasticsearch.yml; service elasticsearch restart'
done

