To use this neo4j plug-in perform the following steps:
1. Copy the file target/neo4jVersionControl-0.0.1-SNAPSHOT-jar-with-dependencies.jar into the plugins folder of your database. This folder is located in your neo4j installation.
2. Start the Neo4j database (close it if it was already running. Be aware that the current version of this plug-in does not work correctly if there is already data in the database.)
(3. if necessary install curl. You can use some different tool to access the REST API of the database but then i can't help you)
4. run this in command line (replace the host, user and password with your own):
curl -X POST -u neo4j:12345 localhost:7474/db/data/ext/VersionControl/graphdb/startVersionControl -H "Content-Type: application/json" -d {\"database_url\":\"localhost:7475\",\"user\":\"neo4j\",\"password\":\"12345\"}
Be aware the escape characters \" may differ depending on your operating system. For further help please use the neo4j documentation (http://neo4j.com/docs/stable/server-plugins.html)