package com.keystone.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.policies.*;
public class CassandraClient {
   private Cluster cluster;
   private Session session;

   public void connect(String node, int port) {
      cluster = Cluster.builder()
            .addContactPoint(node)
            .withPort(port)
            .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
            .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
            // .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
            .build();
      session = cluster.connect();
      Metadata metadata = cluster.getMetadata();
      System.out.printf("Connected to cluster: %s\n", 
            metadata.getClusterName());
      for ( Host host : metadata.getAllHosts() ) {
         System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n",
               host.getDatacenter(), host.getAddress(), host.getRack());
      }
   }

   public Session getSession(){
      return session;
   }

   public ResultSet execute(String query) {
      return session.execute(query);
   }

   public void close() {
      session.close();
      cluster.close();
   }

}