package de.lehrbaum.neo4jVersionControl.restAPI;

import java.util.logging.Logger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

import de.lehrbaum.neo4jVersionControl.ChangeEventListener;
import de.lehrbaum.neo4jVersionControl.io.DatabaseConnection;

public class VersionControl extends ServerPlugin {
	private Logger logger = Logger.getLogger("de.lehrbaum.neo4jVersionControl.restAPI");
	
	private ChangeEventListener listener;
	
	@Description("Start the version control of this database.")
	@PluginTarget(GraphDatabaseService.class)
	public void startVersionControl(@Source GraphDatabaseService graphDb,
		@Description("The url of the backup database. e.g. localhost:7474") 
		@Parameter(name = "database_url") String databaseUrl,
		@Description("The username used to access the database")
		@Parameter(name = "user") String user,
		@Description("The password used to access the database")
		@Parameter(name = "password") String password)
	{
		//TODO: execute sync
		logger.info("start version control called.");
		DatabaseConnection con = new DatabaseConnection(databaseUrl, user, password);
		sync(graphDb, con);
		listener = new ChangeEventListener(con);
		graphDb.registerTransactionEventHandler(listener);
	}
	
	private void sync(GraphDatabaseService graphDb, DatabaseConnection con) {
		//get all nodes and write them. use transactional context
		
	}
	
	@Description("Stop the version control of this database.")
	@PluginTarget(GraphDatabaseService.class)
	public void stopVersionControl(@Source GraphDatabaseService graphDb)
	{
		logger.info("stop version control called.");
		graphDb.unregisterTransactionEventHandler(listener);
		listener = null;
	}
	
	@Name("getStateAtTimestamp")
	@Description("Get a query that extracts the database state at a given timestamp.")
	public String getStateAtTime(
		@Description("The timestamp. Can be a date or time in millisec since 1970.") @Parameter(
			name = "timestamp") String timestamp)
	{
		
		return "MATCH (n) �[r]-> (m) WHERE r.to > " + timestamp + " AND r.from <= " + timestamp
			+ " RETURN n, r, m";
	}
}