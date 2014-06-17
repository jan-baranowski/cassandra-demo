package com.example.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

public class SimpleClient {
	
	public static final int CASSANDRA_NATIVE_TRANSPORT_PORT = 9042;
	
	private Cluster cluster;

	public void connect(String node, int nattedNodesInsideCount) {
		Builder b = Cluster.builder();
		for (int i=0; i<nattedNodesInsideCount; i++) {
			b.withPort(CASSANDRA_NATIVE_TRANSPORT_PORT+i).addContactPoint(node);
		}
		this.cluster = b.build();
		
		Metadata metadata = cluster.getMetadata();
		
		System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());
		for ( Host host : metadata.getAllHosts() ) {
			System.out.printf("Datatacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
		}
	}

	public void close() {
		cluster.close();
	}

	public static void main(String[] args) {
		SimpleClient client = new SimpleClient();
		client.connect("komp7", 3);
		client.close();
	}
}