from tc_analyzer_lib.DB_operations.mongo_neo4j_ops import MongoNeo4jDB


class AnalyzerDBManager:
    def __init__(self):
        """
        base class for the analyzer
        """
        pass

    def database_connect(self):
        """
        Connect to the database
        """
        self.DB_connections = MongoNeo4jDB(testing=False)
        self.DB_connections.set_mongo_db_ops()
