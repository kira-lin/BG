#!/bin/bash

node0=zhilin@amd006.utah.cloudlab.us
node1=zhilin@amd028.utah.cloudlab.us
node2=zhilin@amd017.utah.cloudlab.us

mysql_master_setup () {
    scp config.ini $1:/users/zhilin/config.ini
    ssh $1 'echo "[mysqld]" | sudo tee -a /etc/mysql/mysql.cnf && \
            echo "ndbcluster" | sudo tee -a /etc/mysql/mysql.cnf && \
            echo "[mysql_cluster]" | sudo tee -a /etc/mysql/mysql.cnf && \
            echo "ndb-connectstring=node0" | sudo tee -a /etc/mysql/mysql.cnf && \
            mkdir mysql-cluster'
}

mysql_datanode_setup () {
    ssh $1 'mkdir -p mysql/data'
}

if [ "$1" = "setup" ]
then
    mysql_master_setup $node0
    mysql_datanode_setup $node1
    mysql_datanode_setup $node2
elif [ "$1" = "start" ]
then
    ssh $node0 "nohup sudo ndb_mgmd -f config.ini > /dev/null 2>&1 &"
    ssh $node1 'sudo ndbd -d --connect-string "nodeid=0,node0:1186"'
    ssh $node2 'sudo ndbd -d --connect-string "nodeid=0,node0:1186"'
    ssh $node0 "sudo service mysql restart"
elif [ "$1" = "stop" ]
then
    ssh $node0 "sudo pkill ndb_mgmd"
    ssh $node1 "sudo pkill ndbd"
    ssh $node2 "sudo pkill ndbd"
else
    echo "wrong operation"
fi
