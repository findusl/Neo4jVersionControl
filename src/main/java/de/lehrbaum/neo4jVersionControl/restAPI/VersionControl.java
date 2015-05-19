package de.lehrbaum.neo4jVersionControl.restAPI;

import java.util.logging.Logger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

import de.lehrbaum.neo4jVersionControl.ChangeEventListener;
import de.lehrbaum.neo4jVersionControl.io.DatabaseConnection;

public class VersionControl extends ServerPlugin {
	private Logger logger = Logger.getLogger("de.lehrbaum.neo4jVersionControl.restAPI");
	
	private ChangeEventListener listener;
	
	@Name("start_version_control")
	@Description("Start the version control of this database")
	@PluginTarget(GraphDatabaseService.class)
	public void startVersionControl(@Source GraphDatabaseService graphDb)
	{
		logger.info("start version control called.");
		listener = new ChangeEventListener(new DatabaseConnection());
		graphDb.registerTransactionEventHandler(listener);
	}
	
	@Name("stop_version_control")
	@Description("Stop the version control of this database")
	@PluginTarget(GraphDatabaseService.class)
	public void stopVersionControl(@Source GraphDatabaseService graphDb)
	{
		logger.info("stop version control called.");
		graphDb.unregisterTransactionEventHandler(listener);
		listener = null;
	}
}