#!/bin/bash


for node_ip in "$@"
do
   echo "Copying to $node_ip ..."
   ssh-copy-id root@$node_ip 
done

