package de.lehrbaum.neo4jVersionControl.restAPI;

import java.util.ArrayList;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;
import org.neo4j.tooling.GlobalGraphOperations;

public class GetState extends ServerPlugin {
	
	@Name("get_state_at_time")
	@Description("Get the database state at a time")
	@PluginTarget(GraphDatabaseService.class)
	public Iterable<Node> getStateAtTime(@Source GraphDatabaseService graphDb)
	{
		ArrayList<Node> nodes = new ArrayList<>();
		try (Transaction tx = graphDb.beginTx())
		{
			for (Node node : GlobalGraphOperations.at(graphDb).getAllNodes())
			{
				nodes.add(node);
			}
			tx.success();
		}
		return nodes;
	}
}