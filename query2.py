import sqlite3
import time
import requests
from neo4j import GraphDatabase

con = sqlite3.connect("database.sqlite")
cur = con.cursor()

neo4j_url = "bolt://localhost:7687"  # Adjust the URL based on your Neo4j server configuration
neo4j_auth = ("neo4j", "datamanagement")  # Adjust the username and password

uri = "bolt://localhost:7687"
username = "neo4j"
password = "soccerDB"
driver = GraphDatabase.driver(uri, auth=(username, password))

def measure_Query(num, q_sql, q_neo4j):
    # Esecuzione della query in SQL
    print(f"Executing Query {num} in SQL:")
    begin_time_sql = time.time()
    print(cur.execute(q_sql).fetchall())
    
    
    end_time_sql = time.time()
    print(f"Done! Time elapsed: {(end_time_sql - begin_time_sql) * 1000} ms")

    # Esecuzione della query in Neo4j
    print(f"\nExecuting Query {num} in Neo4j:")
    begin_time_neo4j = time.time()
    with driver.session() as session:
        records = session.run(q_neo4j)
        print( [ dict(i) for i in records] )
        
    end_time_neo4j = time.time()
    print(f"Done! Time elapsed: {(end_time_neo4j - begin_time_neo4j) * 1000} ms")
    
def measure_Query_Multiple(num, q_sql, q_neo4j):
    # Esecuzione della query in SQL
    print(f"Executing Query {num} in SQL:")
    begin_time_sql = time.time()
    print(cur.executescript(q_sql).fetchall())
    
    
    end_time_sql = time.time()
    print(f"Done! Time elapsed: {(end_time_sql - begin_time_sql) * 1000} ms")

    # Esecuzione della query in Neo4j
    print(f"\nExecuting Query {num} in Neo4j:")
    begin_time_neo4j = time.time()
    with driver.session() as session:
        records = session.run(q_neo4j)
        print( [ dict(i) for i in records] )
        
    end_time_neo4j = time.time()
    print(f"Done! Time elapsed: {(end_time_neo4j - begin_time_neo4j) * 1000} ms")
    




# Define your Neo4j queries

# Query 1	---	Return number of matches played by every team in Italy ordered by highest to lowest	---

q1_sql = """
SELECT team.team_long_name, count(*)
FROM country, match, team
WHERE country.id = match.country_id 
AND country.name = "Italy"
AND (match.home_team_api_id = team.team_api_id
OR match.away_team_api_id = team.team_api_id)
GROUP BY team.team_long_name	
ORDER BY count(*) DESC
"""
q1_neo4j = """
MATCH (t:Team)-[r]->(:Match)-[:PLAYED_IN]->(:Country {name:'Italy'})
WHERE type(r) = 'PLAYED_AS_AWAY' OR type(r) = 'PLAYED_AS_HOME'
RETURN t.team_long_name,count(t) ORDER BY count(t) DESC
"""

# Query 2	---	Return name of the team that won more matches	---

q2_sql="""
SELECT team.team_long_name, count(*) as vinte
FROM match, team
WHERE (match.home_team_api_id = team.team_api_id AND match.home_team_goal > match.away_team_goal)
OR (match.away_team_api_id = team.team_api_id AND
match.away_team_goal > match.home_team_goal)
GROUP BY team.team_long_name
ORDER BY vinte desc
LIMIT 1;
"""
q2_neo4j="""
MATCH (s:Team)-[r]->(m:Match)
WHERE (type(r) = 'PLAYED_AS_HOME' AND m.home_team_goal>m.away_team_goal)
OR (type(r) = 'PLAYED_AS_AWAY' AND m.away_team_goal > m.home_team_goal)
RETURN s.team_long_name, count(s) as vinte
ORDER BY vinte DESC LIMIT 1;
"""

# Query 3	---	Return number of teams that have played at least one game in a Spanish League.	---

q3_sql="""
SELECT distinct count(*)
FROM match, league, team, country
WHERE country.name = "Spain" AND country.id = league.country_id AND league.id = match.league_id AND (match.home_team_api_id = team.team_api_id OR match.away_team_api_id = team.team_api_id)
GROUP BY country.name
"""
q3_neo4j="""
MATCH (t:Team)-->(:Match)-[:FOR_LEAGUE]->(:League)-[:BASED_IN]->(:Country {name:'Spain'}) 
RETURN count(t);
"""

# Query 4	---	Return the chain of home-away team (Casa - Trasferta --> Trasferta - Casa) ordered by date of the first match	---

q4_sql="""
SELECT DISTINCT m1.date,t1.team_long_name, t2.team_long_name
FROM team as t1, team as t2, match as m1, match as m2
WHERE 
(m1.away_team_api_id = m2.home_team_api_id)
AND
t2.team_api_id = m2.home_team_api_id
AND
t1.team_api_id = m1.home_team_api_id
ORDER BY m1.date 
"""

q4_neo4j="""
MATCH (t1:Team)-[:PLAYED_AS_HOME]->(m1:Match)<-[:PLAYED_AS_AWAY]-(t2:Team)-[:PLAYED_AS_HOME]->(m2:Match)
RETURN DISTINCT m1.date,t1.team_long_name,t2.team_long_name ORDER BY m1.date
"""

# Query 5	---	Return the chain of home-away teams in the same season where home always won (Casa - Trasferta --> Trasferta - Casa)	---

q5_sql="""
SELECT distinct t1.team_long_name ,t2.team_long_name , t3.team_long_name
FROM team as t1,team as t2, team as t3, match as m1, match as m2, match as m3
WHERE (m1.away_team_api_id = m2.home_team_api_id AND m1.home_team_goal > m1.away_team_goal)
AND
(m2.away_team_api_id = m3.home_team_api_id AND m2.home_team_goal > m2.away_team_goal AND m2.season = m1.season)
AND
(m3.home_team_goal > m3.away_team_goal AND m3.season = m2.season)
AND
t1.team_api_id = m1.home_team_api_id
AND
t2.team_api_id = m2.home_team_api_id
AND
t3.team_api_id = m3.home_team_api_id
"""

q5_neo4j="""
MATCH (t1:Team)-[:PLAYED_AS_HOME]->(m1:Match)<-[:PLAYED_AS_AWAY]-(t2:Team)-[:PLAYED_AS_HOME]->(m2:Match)<-[:PLAYED_AS_AWAY]-(t3:Team)-[:PLAYED_AS_HOME]->(m3:Match)
WHERE m1.home_team_goal > m1.away_team_goal AND m2.home_team_goal>m2.away_team_goal AND m3.home_team_goal>m3.away_team_goal
AND m1.season = m2.season AND m2.season = m3.season
RETURN DISTINCT t1.team_long_name,t2.team_long_name,t3.team_long_name;
"""


# Query 6	---	Drop 'crossing' columns in Player_Attributes 	---

q6_sql = """
ALTER TABLE player_attributes
DROP COLUMN crossing
"""

q6_neo4j = """
MATCH (pa:Player_Attributes) REMOVE pa.crossing
"""

# Query 7	---	Add a new Column that count total goals in each match	---

q7_sql = """
ALTER TABLE match
ADD total_goals SMALLINT;
UPDATE match
SET total_goals = home_team_goal + away_team_goal;
"""

q7_neo4j = """
MATCH (m:Match) SET m.total_goals = m.away_team_goal + m.home_team_goal; 
"""

# Query 8	---	Return first 5 Players with the highest stat overall	---

q8_sql = """
SELECT p.player_name, AVG(pa.overall_rating) AS avg_rating
FROM Player_Attributes pa
JOIN Player p ON pa.player_api_id = p.player_api_id
GROUP BY p.player_api_id
ORDER BY avg_rating DESC
LIMIT 5;
"""

q8_neo4j = """
MATCH (p:Player)-[:HAS_ATTRIBUTES_PLAYER]->(pa:Player_Attributes)
RETURN p.player_name AS player, AVG(pa.overall_rating) AS avg_rating
ORDER BY avg_rating DESC
LIMIT 5;
"""

# Query 9	---	Return total number of matches played in each country	---

q9_sql = """
SELECT c.name AS country, COUNT(*) AS match_count
FROM Country c
JOIN MATCH m ON c.id = m.country_id
GROUP BY c.name;

"""

q9_neo4j = """
MATCH (c:Country)
OPTIONAL MATCH (c)<-[:PLAYED_IN]-(:Match)
WITH c, COUNT(*) AS match_count
RETURN c.name AS country, match_count;

"""

# Query 10	---	Return first 20 matches played in Italy with highest Goal Difference for the home team and a "Chance_Creation_Shooting" value greater than 70.	---
#Highest Diff value in Italy with ChanceShooting greater than 70

q10_sql = """

SELECT 
	t.team_long_name , 
	m.home_team_goal,
	m.away_team_goal ,
	ta.chanceCreationShooting,
	(m.home_team_goal - m.away_team_goal ) AS Diff
FROM 
	team as t
	INNER JOIN match m on t.team_api_id = m.home_team_api_id and (m.home_team_goal - m.away_team_goal) > 2
	INNER JOIN country c on c.id = m.country_id and c.name = "Italy"
	INNER JOIN team_attributes ta on ta.team_api_id = t.team_api_id and ta.chanceCreationShooting > 70
	ORDER BY Diff DESC
	LIMIT 20
"""

q10_neo4j = """
MATCH (t:Team)-[:PLAYED_AS_HOME]->(m:Match)-[:PLAYED_IN]->(c:Country {name: 'Italy'})
MATCH (t)-[:HAS_ATTRIBUTES_TEAM]->(ta:Team_Attributes)
WHERE (m.home_team_goal - m.away_team_goal ) > 2 and ta.chanceCreationShooting > 70
RETURN t.team_long_name,m.home_team_goal , m.away_team_goal , (m.home_team_goal - m.away_team_goal ) as Diff ORDER BY Diff DESC LIMIT 20;

"""

# Query 11	---	Return Average value of (HOME,AWAY) made goals in 2010/2011 season for every league.	---

q11_sql = """

SELECT 
    l.name AS league_name, 
    AVG(m.home_team_goal) AS avg_home_goals, 
    AVG(m.away_team_goal) AS avg_away_goals
FROM Match m
JOIN League l ON m.league_id = l.id
WHERE m.season = '2008/2009'
GROUP BY l.name;
"""

q11_neo4j = """
MATCH (m:Match {season: '2008/2009'})-[:FOR_LEAGUE]->(l:League)
RETURN l.name, SUM(m.home_team_goal) AS total_home_goals, SUM(m.away_team_goal) AS total_away_goals
ORDER BY l.name;

"""

# Query 12	---	Return SUM of goals made in home and away for every league.	---

q12_sql = """

SELECT 
    l.name AS league_name, 
    SUM(m.home_team_goal) AS total_home_goals, 
    SUM(m.away_team_goal) AS total_away_goals
FROM Match m
JOIN League l ON m.league_id = l.id
GROUP BY l.name;

"""

q12_neo4j = """
MATCH (m:Match)-[:FOR_LEAGUE]->(l:League)
RETURN l.name, SUM(m.home_team_goal) AS total_home_goals, SUM(m.away_team_goal) AS total_away_goals
ORDER BY l.name;


"""
#TEST QUERY

qtest_sql = """

SELECT 
    l.name AS league_name, 
    SUM(m.home_team_goal) AS total_home_goals, 
    SUM(m.away_team_goal) AS total_away_goals
FROM Match m
JOIN League l ON m.league_id = l.id
GROUP BY l.name;

"""

qtest_neo4j = """
MATCH (m:Match)-[:FOR_LEAGUE]->(l:League)
RETURN l.name, SUM(m.home_team_goal) AS total_home_goals, SUM(m.away_team_goal) AS total_away_goals
ORDER BY l.name;


"""


# Continue defining other queries (q2_sql, q2_neo4j, q3_sql, q3_neo4j, etc.)

# Execute queries

input("First query: Return number of matches played by every team in Italy ordered by highest to lowest.\nPress enter to start...")
measure_Query("1", q1_sql, q1_neo4j)

input("\nSecond query: Return name of the team that won more matches.\nPress enter to start...")
measure_Query("2",q2_sql,q2_neo4j)

input("\nThird query: Return number of teams that have played at least one game in a Spanish League.\nPress enter to start...")
measure_Query("3",q3_sql,q3_neo4j)

input("\nFourth query: Return the chain of home-away team (Casa - Trasferta --> Trasferta - Casa) ordered by date of the first match.\nPress enter to start...")
measure_Query("4",q4_sql,q4_neo4j)

input("\nFifth query: Return the chain of home-away teams in the same season where home always won (Casa - Trasferta --> Trasferta - Casa).\nPress enter to start...")
measure_Query("5",q5_sql,q5_neo4j)

#input("\nSixth query: Drop 'crossing' column in Player_Attributes.\nPress enter to start...")
#measure_Query("6",q6_sql,q6_neo4j)

#input("\nSeventh query: Add a new Column that counts total goals in each match.\nPress enter to start...")
#measure_Query_Multiple("7",q7_sql,q7_neo4j)

input("\nEigth query: Return first 5 Players with the highest stat overall.\nPress enter to start...")
measure_Query("8",q8_sql,q8_neo4j)

input("\nNinth query: Return total number of matches played in each country.\nPress enter to start...")
measure_Query("9",q9_sql,q9_neo4j)

input("\nTenth query: Return first 20 matches played in Italy with highest Goal Difference for the home team and a 'Chance_Creation_Shooting' value greater than 70.\nPress enter to start...")
measure_Query("10",q10_sql,q10_neo4j)

input("\nEleventh query: Return Average value of (HOME,AWAY) made goals in 2010/2011 season for every league.\nPress enter to start...")
measure_Query("11",q11_sql,q11_neo4j)

input("\nTwelve query: Return SUM of goals made in home and away for every league.\nPress enter to start...")
measure_Query("12",q12_sql,q12_neo4j)



# Close connections
con.close()

