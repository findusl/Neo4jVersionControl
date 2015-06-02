package de.lehrbaum.neo4jVersionControl.io;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.jdbc.Driver;
import org.neo4j.jdbc.Neo4jConnection;

public class DatabaseConnection {
	
	private Logger logger = Logger.getLogger("de.lehrbaum.neo4jVersionControl.io");
	
	/**
	 * A JDBC Neo4j database connection.
	 */
	protected final Neo4jConnection connection;
	
	private final ExecutorService queryExecutor;
	
	/**
	 * Create a new instance of this class and connect to the database. Currently using neo4j:12345
	 * as user:password.
	 * 
	 * @param databaseUrl The URL of the database to connect to. e.g. localhost:7474
	 */
	public DatabaseConnection(String databaseUrl) {
		try {
			Properties prop = new Properties();
			prop.setProperty("user", "neo4j");
			prop.setProperty("password", "12345");
			//TODO: save not executed query in case of error. reconnect.
			connection = new Driver().connect("jdbc:neo4j://" + databaseUrl, prop);
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "Problem when connecting to database.", e);
			throw new IllegalArgumentException(
				"Could not connect to database. See log for more information.");
		}
		queryExecutor = Executors.newSingleThreadExecutor();
	}
	
	public synchronized void executeQuery(CharSequence query) {
		queryExecutor.execute(new queryRunnable(query));
		//use queryExecutor.shutdownNow() in case of connection error.
		//set flag that says if the last query was successful. write unsuccessful operations 
		//in a file on disk to have them permanently.
	}
	
	private class queryRunnable implements Runnable {
		private final CharSequence query;
		
		public queryRunnable(final CharSequence query) {
			this.query = query;
		}
		
		@Override
		public void run() {
			try {
				connection.executeQuery(query.toString(), null);
				logger.log(Level.INFO, "Executed Query: " + query);
			} catch (SQLException e) {
				logger.log(Level.SEVERE, "Problem when executing query: " + query, e);
			}
		}
		
	}
}