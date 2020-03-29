#!/bin/bash

node0=zhilin@amd006.utah.cloudlab.us
node1=zhilin@amd028.utah.cloudlab.us
node2=zhilin@amd017.utah.cloudlab.us
node3=zhilin@amd014.utah.cloudlab.us

install_mongo () {
    ssh $1 "wget -qO - https://www.mongodb.org/static/pgp/server-4.2.asc | sudo apt-key add - && \
            echo \"deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu bionic/mongodb-org/4.2 multiverse\" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.2.list && \
            sudo apt update && \
            sudo apt install -y mongodb-org"
}

install_mysql_master () {
    ssh $1 "wget -q https://dev.mysql.com/get/Downloads/MySQL-Cluster-8.0/mysql-cluster_8.0.19-1ubuntu18.04_amd64.deb-bundle.tar && \
            mkdir mysql_install && \
            tar -xvf mysql-cluster_8.0.19-1ubuntu18.04_amd64.deb-bundle.tar -C mysql_install/ && \
            sudo apt install -y libaio1 libmecab2 && \
            sudo dpkg -i mysql_install/mysql-cluster-community-management-server_8.0.19-1ubuntu18.04_amd64.deb && \
            sudo dpkg -i mysql_install/mysql-common_8.0.19-1ubuntu18.04_amd64.deb && \
            sudo dpkg -i mysql_install/mysql-cluster-community-client-core_8.0.19-1ubuntu18.04_amd64.deb && \
            sudo dpkg -i mysql_install/mysql-cluster-community-client_8.0.19-1ubuntu18.04_amd64.deb && \
            sudo dpkg -i mysql_install/mysql-client_8.0.19-1ubuntu18.04_amd64.deb && \
            sudo dpkg -i mysql_install/mysql-cluster-community-server-core_8.0.19-1ubuntu18.04_amd64.deb && \
            sudo dpkg -i mysql_install/mysql-cluster-community-server_8.0.19-1ubuntu18.04_amd64.deb && \
            sudo dpkg -i mysql_install/mysql-server_8.0.19-1ubuntu18.04_amd64.deb"
}

install_mysql_datanode () {
    ssh $1 "wget https://dev.mysql.com/get/Downloads/MySQL-Cluster-8.0/mysql-cluster-community-data-node_8.0.19-1ubuntu18.04_amd64.deb && \
            sudo apt install libclass-methodmaker-perl && \
            sudo dpkg -i mysql-cluster-community-data-node_8.0.19-1ubuntu18.04_amd64.deb"
}

install_mongo $node0
install_mongo $node1
install_mongo $node2
install_mysql_master $node0
install_mysql_datanode $node1
install_mysql_datanode $node2
