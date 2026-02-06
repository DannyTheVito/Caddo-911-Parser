import osmnx as ox
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import text
import config 

DB_USER = config.databaseuser
DB_PASS = config.databasepasswd
DB_HOST = config.databasehost
DB_NAME = config.databasename

def build_intersection_db():
    print("Downloading Caddo Parish road network (this may take a minute)...")
    
    # Filter for 'drive' to get drivable roads
    graph = ox.graph_from_place("Caddo Parish, Louisiana, USA", network_type='drive')
    
    # Convert the graph into geodataframes
    nodes, edges = ox.graph_to_gdfs(graph)
    
    intersections = []

    print("Parsing intersections and street names...")
    
    for node_id, row in nodes.iterrows():
        connected_edges = edges.loc[[node_id] if isinstance(edges.index, str) else edges.index.get_level_values(0) == node_id]
        
        if 'name' in connected_edges.columns:
            names = connected_edges['name'].dropna().tolist()
            flat_names = []
            for n in names:
                if isinstance(n, list): flat_names.extend(n)
                else: flat_names.append(n)
            
            unique_streets = sorted(list(set(flat_names)))

            if len(unique_streets) >= 2:
                for i in range(len(unique_streets)):
                    for j in range(i + 1, len(unique_streets)):
                        intersections.append({
                            'street_a': unique_streets[i].upper(),
                            'street_b': unique_streets[j].upper(),
                            'lat': row['y'],
                            'lon': row['x'],
                            'node_id': node_id
                        })

    print(f"Found {len(intersections)} unique intersection pairs.")

    print("Uploading to MySQL...")
    
    engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")
    
    df = pd.DataFrame(intersections)
    
    df = df.drop_duplicates(subset=['street_a', 'street_b'])
    
    df.to_sql('osm_intersections', con=engine, if_exists='replace', index=False)
    
    with engine.begin() as conn:  
        conn.execute(text("CREATE INDEX idx_st_a ON osm_intersections (street_a(50))"))
        conn.execute(text("CREATE INDEX idx_st_b ON osm_intersections (street_b(50))"))
        print("Created indexes on street_a and street_b.")

if __name__ == "__main__":
    build_intersection_db()