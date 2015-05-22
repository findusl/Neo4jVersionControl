package de.lehrbaum.neo4jVersionControl;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import de.lehrbaum.neo4jVersionControl.io.DatabaseConnection;

public class ChangeEventListener implements TransactionEventHandler<Void> {
	
	private Logger logger = Logger.getLogger("de.lehrbaum.neo4jVersionControl");
	
	protected DatabaseConnection dbConnection;
	
	public ChangeEventListener(DatabaseConnection dbConnection) {
		this.dbConnection = dbConnection;
		logger.setLevel(Level.ALL);
	}
	
	@Override
	public void afterCommit(TransactionData data, Void arg1) {
		logger.info("After commit called.");
		// write changes to database
		// maybe use multithreading for different attribute types
		try {
			long timestamp = System.currentTimeMillis();
			writeChangedNodes(data.createdNodes(), timestamp, false);
			writeChangedNodeProperties(data.assignedNodeProperties(), timestamp, false);
			writeChangedNodeProperties(data.removedNodeProperties(), timestamp, true);
			writeChangeNodeLabels(data.assignedLabels(), timestamp, false);
			writeChangeNodeLabels(data.removedLabels(), timestamp, true);
			
			//TODO: deal with Relationships
			writeChangedNodes(data.deletedNodes(), timestamp, true);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Problem", e);
		}
	}
	
	private void writeChangedNodes(Iterable<Node> nodes, long timestamp, boolean remove) {
		for (Node n : nodes) {
			String query;
			if (!remove)
				query = "MERGE (idNode:VCSID {id:" + n.getId() + "}) CREATE ()-[:VCShas {from:"
					+ timestamp + ", to:" + Long.MAX_VALUE + "}]->idNode";
			else {
				query = matchNode(n.getId()) + "SET r1.to=" + timestamp;
			}
			logger.info("My query: " + query);
		}
	}
	
	private void writeChangedNodeProperties(Iterable<PropertyEntry<Node>> properties,
		long timestamp, boolean remove) {
		for (PropertyEntry<Node> entry : properties) {
			/*MATCH (n)-[:VCShas {to:max}]->(:VCSID {id:nodeid})
			MATCH n-[r2:VCShas {to:max}]->(p2:VCSProperty {prop_name:prop_old_value})
			SET r2.to=curr
			MERGE (p1:VCSProperty {prop_name:prop_new_value})
			CREATE n-[:VCShas {from:curr, to:max}]->p1
			*/
			Node n = entry.entity();
			StringBuilder query = new StringBuilder();
			query.append(matchNode(n.getId()));
			if (entry.previouslyCommitedValue() != null) {
				query.append(setProperty(timestamp, true, entry.key(), propertyString(entry.value())));
			}
			if (!remove) {
				query.append(setProperty(timestamp, false, entry.key(), propertyString(entry.value())));
			}
			logger.info("My query: " + query);
		}
	}
	
	private void writeChangeNodeLabels(Iterable<LabelEntry> assignedLabels, long timestamp,
		boolean remove) {
		for (LabelEntry entry : assignedLabels) {
			Label label = entry.label();
			Node n = entry.node();
			StringBuilder query = new StringBuilder();
			query.append(matchNode(n.getId()));
			query.append(setProperty(timestamp, remove, "VCSLabel", label.name()));
			logger.info("My query: " + query);
		}
	}
	
	//some Methods creating parts of a query:
	
	private StringBuilder matchNode(long id) {
		return matchNode(id, "n");
	}
	
	private StringBuilder matchNode(long id, CharSequence nodeIdentifier) {
		StringBuilder sb = new StringBuilder();
		sb.append("MATCH (").append(nodeIdentifier).append(")-[r1:VCShas {to:")
			.append(Long.MAX_VALUE).append("}]->(:VCSID {id:").append(id).append("}) ");
		return sb;
	}
	
	private StringBuilder setProperty(long timestamp, boolean remove, CharSequence propName,
		CharSequence propValue) {
		StringBuilder sb = new StringBuilder();
		sb.append("MERGE (p:VCSProperty {").append(propName).append(':').append(propValue);
		if (remove) {
			sb.append("}) MATCH n-[r2:VCShas {to:").append(Long.MAX_VALUE).append("}]->p")
				.append(" SET r2.to=").append(timestamp);
		}
		else
			sb.append("}) CREATE n-(:VCShas {from:").append(timestamp).append(", to:")
				.append(Long.MAX_VALUE).append("})->p ");
		return sb;
	}
	
	/**
	 * Converts the property into a string usable as input to a neo4j database.
	 * 
	 * Returning StringBuilder for performance issues. Is faster when being further concatenated.
	 * 
	 * @param property
	 * @return
	 */
	public static StringBuilder propertyString(Object property) {
		return new StringBuilder().append('"').append(property.toString()).append('"');
	}
	
	@Override
	public void afterRollback(TransactionData data, Void state) {
		// don't care
	}
	
	@Override
	public Void beforeCommit(TransactionData data) throws Exception {
		logger.info("Before commit called");
		// don't care
		return null;
	}
	
}