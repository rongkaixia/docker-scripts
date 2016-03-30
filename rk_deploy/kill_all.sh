#!/bin/bash

function kill_containers() {
    containers=($1)
    for i in "${containers[@]}"; do
        echo "killing container $i"
        sudo docker kill "$i"
    done
}

if [ "$#" -ne "1" ]; then
    echo -e "usage:\n   $0 spark\n   $0 shark\n   $0 mesos\n   $0 nameserver"
    exit 1;
fi

if [[ "$USER" != "root" ]]; then
   echo "please run as: sudo $0"
   exit 1
fi

clustertype=$1

if [[ "$clustertype" == "nameserver" ]]; then
    nameserver=$(sudo docker ps | grep dnsmasq_files | awk '{print $1}' | tr '\n' ' ')
    kill_containers "$nameserver"
else
    nameserver=$(sudo docker ps | grep dnsmasq_files | awk '{print $1}' | tr '\n' ' ')
    master=$(sudo docker ps | grep ${clustertype}-master | awk '{print $1}' | tr '\n' ' ')
    workers=$(sudo docker ps | grep ${clustertype}-worker | awk '{print $1}' | tr '\n' ' ')
    shells=$(sudo docker ps | grep ${clustertype}-shell | awk '{print $1}' | tr '\n' ' ')
    cassandra=$(sudo docker ps | grep cassandra | awk '{print $1}' | tr '\n' ' ')
    kill_containers "$nameserver"
    kill_containers "$master"
    kill_containers "$workers" 
    kill_containers "$shells"
    kill_containers "$cassandra"
fi

