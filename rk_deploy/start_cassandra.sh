#!/bin/bash

CASSANDRA=-1
CASSANDRA_IP=

# starts the cassandra container
function start_cassandra() {
    echo "starting cassandra container"
    if [ "$DEBUG" -gt 0 ]; then
        echo sudo docker run -d --dns $NAMESERVER_IP -h cassandra${DOMAINNAME} $VOLUME_MAP $1:$2
    fi
    CASSANDRA=$(docker run -d --dns $NAMESERVER_IP -h cassandra${DOMAINNAME} $VOLUME_MAP $1:$2)

    if [ "$CASSANDRA" = "" ]; then
        echo "error: could not start cassandra container from image $1:$2"
        exit 1
    fi

    echo "started cassandra container:      $CASSANDRA"
    sleep 3
    CASSANDRA_IP=$(sudo docker inspect --format '{{ .NetworkSettings.IPAddress }}' $CASSANDRA)
    echo "CASSANDRA_IP:                     $CASSANDRA_IP"
    echo "address=\"/cassandra/$CASSANDRA_IP\"" >> $DNSFILE
}

function wait_for_cassandra {
    if [[ "$CASSANDRA_VERSION" == "2.2" ]]; then
        query_string="Starting listening for CQL clients"
    else
        query_string="Starting listening for CQL clients"
    fi
    echo -n "waiting for cassandra "
    sudo docker logs $CASSANDRA | grep "$query_string" > /dev/null
    until [ "$?" -eq 0 ]; do
        echo -n "."
        sleep 1
        sudo docker logs $CASSANDRA | grep "$query_string" > /dev/null;
    done
    echo ""
    echo -n "waiting for nameserver to find cassandra "
    check_hostname result cassandra "$CASSANDRA_IP"
    until [ "$result" -eq 0 ]; do
        echo -n "."
        sleep 1
        check_hostname result cassandra "$CASSANDRA_IP"
    done
    echo ""
    sleep 3
}
