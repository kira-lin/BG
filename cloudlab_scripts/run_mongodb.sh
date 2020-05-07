#!/bin/bash

mongod_repl () {
    scp -P $2 mongod_repl.conf $1:/users/zhilin/mongod_repl.conf
    ssh -p $2 $1 "mkdir mongodb && \
                  mkdir mongodb_log && \
                  touch mongodb_log/mongod.log"
    ssh -p $2 $1 "mongod --config mongod_repl.conf"
}

source ./nodes

if [ "$1" = "repl" ]
then
    mongod_repl $node0 $port
    mongod_repl $node1 $port
    mongod_repl $node2 $port
    ssh -p $port $node0 'mongo --eval "rs.initiate( { \
    _id : \"rs0\", \
    members: [ \
        { _id: 0, host: \"node0:27017\" }, \
        { _id: 1, host: \"node1:27017\" }, \
        { _id: 2, host: \"node2:27017\" } \
    ] \
    })"'
elif [ "$1" = "stop" ]
then
    ssh -p $port $node0 "sudo kill \$(cat mongod.pid) && rm mongod.pid"
    ssh -p $port $node1 "sudo kill \$(cat mongod.pid) && rm mongod.pid"
    ssh -p $port $node2 "sudo kill \$(cat mongod.pid) && rm mongod.pid"
else 
    echo "not implemented"
fi
