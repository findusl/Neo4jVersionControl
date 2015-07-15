package de.lehrbaum.neo4jVersionControl;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import de.lehrbaum.neo4jVersionControl.ChangeEventListener.RelationshipData;
import de.lehrbaum.neo4jVersionControl.io.DatabaseConnection;

public class ChangeEventListener implements TransactionEventHandler<Map<Long, RelationshipData>> {
	
	private Logger logger = Logger.getLogger("de.lehrbaum.neo4jVersionControl");
	
	/**
	 * A jdbc connection to the VCS database.
	 */
	protected DatabaseConnection dbConnection;
	
	public ChangeEventListener(DatabaseConnection dbConnection) {
		this.dbConnection = dbConnection;
		logger.setLevel(Level.ALL);
	}
	
	@Override
	public void afterCommit(TransactionData data, Map<Long, RelationshipData> relationshipNodes) {
		// write changes to database
		// maybe use multithreading for different attribute types
		try {
			long timestamp = System.currentTimeMillis();
			writeChangedNodes(data.createdNodes(), timestamp, false);
			writeChangedNodeProperties(data.assignedNodeProperties(), timestamp, false);
			writeChangedNodeProperties(data.removedNodeProperties(), timestamp, true);
			writeChangedNodeLabels(data.assignedLabels(), timestamp, false);
			writeChangedNodeLabels(data.removedLabels(), timestamp, true);
			writeChangedRelationships(data.createdRelationships(), timestamp, relationshipNodes);
			writeChangedRelationshipProperties(data.assignedRelationshipProperties(), timestamp, false);
			writeChangedRelationshipProperties(data.removedRelationshipProperties(), timestamp, true);
			writeChangedRelationships(data.deletedRelationships(), timestamp, null);
			writeChangedNodes(data.deletedNodes(), timestamp, true);
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Problem", e);
		}
	}
	
	//================================================================================
	// Node methods
	//================================================================================
	
	private void writeChangedNodes(Iterable<Node> nodes, long timestamp, boolean remove) {
		for (Node n : nodes) {
			String query;
			if (!remove)
				query = "MERGE (idNode:VCSID {id:" + n.getId() + "}) CREATE (:VCSNode)-[:VCShas {from:"
					+ timestamp + ", to:" + Long.MAX_VALUE + "}]->idNode";
			else {
				query = matchNode(n.getId()).append("SET r1.to=").append(timestamp).toString();
			}
			dbConnection.executeQuery(query);
		}
	}
	
	private void writeChangedNodeProperties(Iterable<PropertyEntry<Node>> properties,
		long timestamp, boolean remove) {
		for (PropertyEntry<Node> entry : properties) {
			/*
			 * MATCH (n:VCSNode)-[r1:VCShas {to:9223372036854775807}]->(:VCSID {id:0})
			 * MERGE (p:VCSProperty {name:"John"}) WITH n, p
			 * MATCH n-[r2:VCShas {to:9223372036854775807}]->p 
			 * SET r2.to=1432506654808 
			 * MERGE (p2:VCSProperty {name:"Max"}) 
			 * CREATE n-[:VCShas {from:1432506654808, to:9223372036854775807}]->p2
			 */
			Node n = entry.entity();
			StringBuilder query = new StringBuilder(210);
			query.append(matchNode(n.getId()));
			Object previousValue = entry.previouslyCommitedValue();
			if (previousValue != null) {
				query.append(setProperty(timestamp, true, entry.key(), propertyString(previousValue)));
			}
			if (!remove) {
				query.append(setProperty(timestamp, false, entry.key(), propertyString(entry.value()),
					"p2"));
			}
			dbConnection.executeQuery(query);
		}
	}
	
	private void writeChangedNodeLabels(Iterable<LabelEntry> assignedLabels, long timestamp,
		boolean remove) {
		for (LabelEntry entry : assignedLabels) {
			Label label = entry.label();
			Node n = entry.node();
			StringBuilder query = new StringBuilder();
			query.append(matchNode(n.getId()));
			query.append(setProperty(timestamp, remove, "VCSLabel", propertyString(label.name())));
			dbConnection.executeQuery(query);
		}
	}
	
	private StringBuilder matchNode(long id) {
		return matchNode(id, "n");
	}
	
	private StringBuilder matchNode(long id, CharSequence nodeIdentifier) {
		StringBuilder sb = new StringBuilder(75);
		sb.append("MATCH (").append(nodeIdentifier).append(":VCSNode)-[r1:VCShas {to:")
			.append(Long.MAX_VALUE).append("}]->(:VCSID {id:").append(id).append("}) ");
		return sb;
	}
	
	private StringBuilder setProperty(long timestamp, boolean remove, CharSequence propName,
		CharSequence propValue) {
		return setProperty(timestamp, remove, propName, propValue, "p");
	}
	
	private StringBuilder setProperty(long timestamp, boolean remove, CharSequence propName,
		CharSequence propValue, CharSequence propNodeId) {
		return setProperty(timestamp, remove, propName, propValue, propNodeId, "n");
	}
	
	private StringBuilder setProperty(long timestamp, boolean remove, CharSequence propName,
		CharSequence propValue, CharSequence propNodeId, CharSequence nodeId) {
		StringBuilder sb = new StringBuilder(60);
		sb.append("MERGE (").append(propNodeId).append(":VCSProperty {").append(propName).append(':')
			.append(propValue);
		if (remove) {
			sb.append("}) WITH ").append(nodeId).append(',').append(propNodeId)
				.append(" MATCH n-[r2:VCShas {to:").append(Long.MAX_VALUE).append("}]->p")
				.append(" SET r2.to=").append(timestamp).append(' ');
		}
		else
			sb.append("}) CREATE ").append(nodeId).append("-[:VCShas {from:").append(timestamp)
				.append(", to:").append(Long.MAX_VALUE).append("}]->").append(propNodeId).append(' ');
		return sb;
	}
	
	//================================================================================
	// Relationship methods
	//================================================================================
	
	@Override
	public Map<Long, RelationshipData> beforeCommit(TransactionData data) throws Exception {
		/*
		 * Need to get the nodes before commit because i can't afterwards.
		 */
		HashMap<Long, RelationshipData> relationshipNodes = new HashMap<Long, RelationshipData>();
		for (Relationship r : data.createdRelationships()) {
			RelationshipData rData = new RelationshipData(propertyString(r.getType().name()),
				r.getStartNode().getId(), r.getEndNode().getId());
			relationshipNodes.put(r.getId(), rData);
		}
		return relationshipNodes;
	}
	
	private void writeChangedRelationships(Iterable<Relationship> relationships,
		long timestamp, Map<Long, RelationshipData> relationshipNodes) {
		if (relationshipNodes != null) {
			for (Relationship r : relationships) {
				StringBuilder query = new StringBuilder();
				RelationshipData rData = relationshipNodes.get(r.getId());
				query.append(matchNode(rData.startNodeId, "ns"));
				query.append(matchNode(rData.endNodeId, "ne"));
				query.append("CREATE ns-[:VCSRelationship {VCSID:").append(r.getId()).append(", from:")
					.append(timestamp).append(", to:").append(Long.MAX_VALUE).append(", VCSType:")
					.append(rData.type).append("}]->ne ");
				
				dbConnection.executeQuery(query);
			}
		} else {
			for (Relationship r : relationships) {
				StringBuilder query = new StringBuilder();
				query.append(matchRelationship(r));
				query.append("SET r.to=").append(timestamp);
				
				dbConnection.executeQuery(query);
			}
		}
	}
	
	private void writeChangedRelationshipProperties(
		Iterable<PropertyEntry<Relationship>> relationships, long timestamp,
		boolean delete) {
		for (PropertyEntry<Relationship> entry : relationships) {
			StringBuilder query = new StringBuilder(60).append("MERGE (nRel:VCSRel{id:")
				.append(entry.entity().getId()).append("}) ");
			Object prevEntry = entry.previouslyCommitedValue();
			//delete old entry
			if (prevEntry != null) {
				query.append(setRelationshipProperty(timestamp, true,
					entry.key(), propertyString(prevEntry)));
			}
			//set new entry
			if (!delete) {
				query.append(setRelationshipProperty("nRel", timestamp, false,
					entry.key(), propertyString(entry.value()), "p2"));
			}
		}
	}
	
	private StringBuilder matchRelationship(Relationship r) {
		StringBuilder sb = new StringBuilder();
		sb.append("MATCH ()-[r {id:").append(r.getId()).append(", to:").append(Long.MAX_VALUE)
			.append("}]->() ");
		return sb;
	}
	
	private StringBuilder setRelationshipProperty(long timestamp, boolean remove,
		CharSequence propName, CharSequence propValue) {
		return setRelationshipProperty("nRel", timestamp, remove, propName, propValue, "p");
	}
	
	private StringBuilder setRelationshipProperty(CharSequence relNodeId, long timestamp,
		boolean remove,
		CharSequence propName, CharSequence propValue, CharSequence propNodeId) {
		StringBuilder sb = new StringBuilder();
		/*
		 * MERGE (p:VCSProperty {prop:value})
		 * CREATE nRel-[:VCShas {from:timestamp, to:max}]->p
		 */
		sb.append("MERGE (").append(propNodeId).append(":VCSProperty {").append(propName).append(':')
			.append(propValue);
		if (remove) {
			sb.append("}) MATCH ").append(relNodeId).append("-[r3").append(":VCShas {to:")
				.append(Long.MAX_VALUE).append("}]->").append(propNodeId);
			sb.append(" SET r3.to=").append(timestamp).append(' ');
		} else {
			sb.append("}) CREATE ").append(relNodeId).append("-[").append(":VCShas {from:")
				.append(timestamp).append(", to:").append(Long.MAX_VALUE).append("}]->")
				.append(propNodeId);
		}
		return sb;
	}
	
	static class RelationshipData {
		CharSequence type;
		long startNodeId;
		long endNodeId;
		
		public RelationshipData(CharSequence type, long startNodeId, long endNodeId) {
			super();
			this.type = type;
			this.startNodeId = startNodeId;
			this.endNodeId = endNodeId;
		}
	}
	
	//================================================================================
	// helper methods/classes
	//================================================================================
	
	/**
	 * Converts the property into a string usable as input to a neo4j database.
	 * <p>
	 * Returning StringBuilder for performance reasons. Is faster when being further concatenated.
	 * 
	 * @category Helper
	 * 
	 * @param property A property returned by a {@link TransactionData} object.
	 * 
	 * @return A representation of the property that can be used for cypher queries.
	 */
	public StringBuilder propertyString(Object property) {
		String stringValue = property.toString();
		stringValue = stringValue.replace("'", "\\'");
		//inefficiently creating one string to much. maybe somehow override it to directly return the StringBuilder before making string out of it.
		
		return new StringBuilder().append('"').append(stringValue).append('"');
	}
	
	@Override
	public void afterRollback(TransactionData data, Map<Long, RelationshipData> relationshipNodes) {
		// don't care
	}
	
}