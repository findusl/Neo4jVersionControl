package de.lehrbaum.neo4jVersionControl.io;

import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.jdbc.Driver;
import org.neo4j.jdbc.Neo4jConnection;

public class DatabaseConnection {
	
	private Logger logger = Logger.getLogger("de.lehrbaum.neo4jVersionControl.io");
	
	protected Neo4jConnection connection;
	
	public DatabaseConnection(String databaseUrl) {
		try {
			Properties prop = new Properties();
			prop.setProperty("user", "neo4j");
			prop.setProperty("password", "12345");
			Class.forName("org.neo4j.jdbc.Driver");
			connection = new Driver().connect("jdbc:neo4j://" + databaseUrl, prop);
		} catch (SQLException | ClassNotFoundException e) {
			logger.log(Level.SEVERE, "Problem when connecting to database.", e);
		}
	}
	
	public synchronized void executeQuery(CharSequence query) {
		try {
			connection.executeQuery(query.toString(), null);
			logger.log(Level.INFO, "Executed Query: " + query);
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Problem when executing query: " + query, e);
		}
	}
	
}