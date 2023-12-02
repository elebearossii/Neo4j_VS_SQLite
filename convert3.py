from neo4j import GraphDatabase
import sqlite3

# Connettersi al database SQLite
con = sqlite3.connect("database.sqlite")
cur = con.cursor()

# Connettersi al database Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "soccerDB"
driver = GraphDatabase.driver(uri, auth=(username, password))

# Definire il nome delle tabelle
table_names = ["Country", "League", "Match", "Player", "Player_Attributes", "Team", "Team_Attributes"]

# Inizializzare la sessione Neo4j
with driver.session() as session:
    for table in table_names:
        data = cur.execute("SELECT * FROM " + table)
        col_names = [col[0] for col in cur.description]
        print("Parsing table " + table + ". Column names: " + str(col_names))
        print("\n\n")

        for row in data:
            row_dict = {}
            for i, att in enumerate(col_names):
                row_dict[att] = row[i]

            # Creare un nodo per ogni riga e inserirlo nel database Neo4j
            query = f"CREATE (n:{table} $props)"
            session.run(query, props=row_dict)

        print(f"Finished inserting data from the {table} table.")

    # Creare la relazione BASED_IN tra League e Country
    query = """
    MATCH (l:League), (c:Country)
    WHERE l.country_id = c.id
    CREATE (l)-[:BASED_IN]->(c)
    """
    session.run(query)
    print("Created BASED_IN relationships.")

    # Creare la relazione PLAYED_IN tra Match e Country
    query = """
    MATCH (m:Match), (c:Country)
    WHERE m.country_id = c.id
    CREATE (m)-[:PLAYED_IN]->(c)
    """
    session.run(query)
    print("Created PLAYED_IN relationships.")

    # Creare la relazione FOR_LEAGUE tra Match e League
    query = """
    MATCH (m:Match), (l:League)
    WHERE m.league_id = l.id
    CREATE (m)-[:FOR_LEAGUE]->(l)
    """
    session.run(query)
    print("Created FOR_LEAGUE relationships.")

    # Creare la relazione PLAYED_AS_HOME tra Team e Match
    query = """
    MATCH (t:Team), (m:Match)
    WHERE t.team_api_id = m.home_team_api_id
    CREATE (t)-[:PLAYED_AS_HOME]->(m)
    """
    session.run(query)
    print("Created PLAYED_AS_HOME relationships.")

    # Creare la relazione PLAYED_AS_AWAY tra Team e Match
    query = """
    MATCH (t:Team), (m:Match)
    WHERE t.team_api_id = m.away_team_api_id
    CREATE (t)-[:PLAYED_AS_AWAY]->(m)
    """
    session.run(query)
    print("Created PLAYED_AS_AWAY relationships.")

    # Creare la relazione HAS_ATTRIBUTES_TEAM tra Team e Team_Attributes
    query = """
    MATCH (t:Team), (ta:Team_Attributes)
    WHERE t.team_api_id = ta.team_api_id
    CREATE (t)-[:HAS_ATTRIBUTES_TEAM]->(ta)
    """
    session.run(query)
    print("Created HAS_ATTRIBUTES_TEAM relationships.")
    
    # Creare la relazione HAS_ATTRIBUTES_PLAYER tra Player e Player_Attributes
    query = """
    MATCH (p:Player), (pa:Player_Attributes)
    WHERE p.player_api_id = pa.player_api_id
    CREATE (p)-[:HAS_ATTRIBUTES_PLAYER]->(pa)
    """
    session.run(query)
    print("Created HAS_ATTRIBUTES_PLAYER relationships.")

# Chiudere la connessione a Neo4j
driver.close()

print("Data import and relationship creation completed.")

