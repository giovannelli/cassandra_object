if [ -n "$ACTIVEMODEL_VERSION" ];then 
  bundle add activemodel --version "$ACTIVEMODEL_VERSION"; 
fi
bundle install
if [ -n "$CASSANDRA_VERSION" ];then 
  ccm create -n 1 -v $CASSANDRA_VERSION -i 127.0.0. -s -b test-cluster; 
  ccm start;
fi
if [ -n "$SCYLLA_VERSION" ];then 
  SCYLLA_IMAGE=scylladb/scylla:$SCYLLA_VERSION; 
  docker pull $SCYLLA_IMAGE;
  docker run --name cassandra_test -d -p "9042:9042" -p "9160:9160" $SCYLLA_IMAGE;
  until docker exec -it cassandra_test nodetool status | grep UN; do : ; done;
fi