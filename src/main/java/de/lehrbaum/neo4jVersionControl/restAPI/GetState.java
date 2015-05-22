package de.lehrbaum.neo4jVersionControl.restAPI;

import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.ServerPlugin;

public class GetState extends ServerPlugin {
	
	@Name("get_state_at_timestamp")
	@Description("Get a query that extracts the database state at a given timestamp.")
	public String getStateAtTime(
		@Description("The timestamp. Can be a date or time in millisec since 1970.") @Parameter(
			name = "timestamp") String timestamp)
	{
		
		return "MATCH (n) –[r]-> (m) WHERE r.to > " + timestamp + " AND r.from <= "
			+ " RETURN n, r, m";
	}
}