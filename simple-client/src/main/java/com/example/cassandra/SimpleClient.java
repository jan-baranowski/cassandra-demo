package com.example.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;

public class SimpleClient {

	public static final int CASSANDRA_NATIVE_TRANSPORT_PORT = 9042;
	
	private Cluster cluster;
	private Session session;
	
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
		
		this.session = cluster.connect();
		}
	
	 
	
	public void close() {
	      cluster.close();
	 }

	   public void createSchema() {
		   session.execute("CREATE KEYSPACE simplex WITH replication " + 
				      "= {'class':'SimpleStrategy', 'replication_factor':3};");
		   session.execute(
				      "CREATE TABLE simplex.songs (" +
				            "id uuid PRIMARY KEY," + 
				            "title text," + 
				            "album text," + 
				            "artist text," + 
				            "tags set<text>," + 
				            "data blob" + 
				            ");");
				session.execute(
				      "CREATE TABLE simplex.playlists (" +
				            "id uuid," +
				            "title text," +
				            "album text, " + 
				            "artist text," +
				            "song_id uuid," +
				            "PRIMARY KEY (id, title, album, artist)" +
				            ");");					   
	   }
	   
	   public void loadData() {
		   session.execute(
				      "INSERT INTO simplex.songs (id, title, album, artist, tags) " +
				      "VALUES (" +
				          "756716f7-2e54-4715-9f00-91dcbea6cf50," +
				          "'La Petite Tonkinoise'," +
				          "'Bye Bye Blackbird'," +
				          "'Jos�phine Baker'," +
				          "{'jazz', '2013'})" +
				          ";");
				session.execute(
				      "INSERT INTO simplex.playlists (id, song_id, title, album, artist) " +
				      "VALUES (" +
				          "2cc9ccb7-6221-4ccb-8387-f22b6a1b354d," +
				          "756716f7-2e54-4715-9f00-91dcbea6cf50," +
				          "'La Petite Tonkinoise'," +
				          "'Bye Bye Blackbird'," +
				          "'Jos�phine Baker'" +
				          ");");
	   }
	   
	   public void querySchema(){
		   ResultSet results = session.execute("SELECT * FROM simplex.playlists " +
			        "WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		   System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
			       "-------------------------------+-----------------------+--------------------"));
			for (Row row : results) {
			    System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
			    row.getString("album"),  row.getString("artist")));
			}
			System.out.println();
	   }
	   
	   public void dropSchema(){
		   session.execute(
				      "DROP KEYSPACE IF EXISTS simplex;");
	   }
	   
	   
	   public static void main(String[] args) {
			SimpleClient client = new SimpleClient();
			client.connect("localhost", 3);
			client.dropSchema();
			client.createSchema();
			
			client.loadData();
			client.querySchema();
			client.close();
	   }
}
