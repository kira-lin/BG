#!/bin/bash

node0=zhilin@amd006.utah.cloudlab.us
node1=zhilin@amd028.utah.cloudlab.us
node2=zhilin@amd017.utah.cloudlab.us

mongod_repl () {
    scp mongod_repl.conf $1:/users/zhilin/mongod_repl.conf
    ssh $1 "mkdir mongodb && mkdir mongodb_log && touch mongodb_log/mongod.log"
    ssh $1 "mongod --config mongod_repl.conf"
}

if [ "$1" = "repl" ]
then
    mongod_repl $node0
    mongod_repl $node1
    mongod_repl $node2
    ssh $node0 'mongo --eval "rs.initiate( { \
    _id : \"rs0\", \
    members: [ \
        { _id: 0, host: \"node0:27017\" }, \
        { _id: 1, host: \"node1:27017\" }, \
        { _id: 2, host: \"node2:27017\" } \
    ] \
    })"'
elif [ "$1" = "stop" ]
then
    ssh $node0 "sudo kill \$(cat mongod.pid) && rm mongod.pid"
    ssh $node1 "sudo kill \$(cat mongod.pid) && rm mongod.pid"
    ssh $node2 "sudo kill \$(cat mongod.pid) && rm mongod.pid"
else 
    echo "not implemented"
fi
