import psycopg2
import os
from collections import defaultdict
from dotenv import load_dotenv
import json

load_dotenv()

def init_keyword_cluster(artifact_ids, cur):
    visited_artifacts = set()

    
    for (artifact,) in artifact_ids:
        if artifact in visited_artifacts:
            continue
        edge_list = []
        keyword_list = set()
        seen_edges = set()
        recurse_keywords(cur, artifact, edge_list, seen_edges, keyword_list, visited_artifacts)
        print("  Keywords:", keyword_list)
        print("  Edges:", edge_list)
        if(len(edge_list) != 0):
            cur.execute("""
                        INSERT INTO keyword_clusters(cluster_name, edges)
                        VALUES(%s, %s);
                        """, (",".join(sorted(keyword_list)), json.dumps(edge_list)))
        conn.commit()

def recurse_keywords(cur, artifact_id, edge_list, seen_edges, keywords, visited_artifacts):
    if artifact_id in visited_artifacts:
        print("already in there")
        return
    visited_artifacts.add(artifact_id)
    common_artifacts = find_artifacts_with_shared_keywords(cur, artifact_id)
    print(common_artifacts)
    if len(common_artifacts) == 0:
        return
    
    
    for artifact in common_artifacts:
        edge_key = tuple(sorted([artifact_id, artifact]))
        if edge_key in seen_edges:
            continue
        edge_list.append({"node1": artifact_id, "node2": artifact})
        keywords.update(common_artifacts[artifact])
        
        recurse_keywords(cur, artifact, edge_list, seen_edges,keywords, visited_artifacts)
        
def find_artifacts_with_shared_keywords(cur, artifact_id):
    ## query returns the artifacts that share the keyword along with the keyword that they share
    cur.execute("""
        WITH artifact_keywords AS (
            SELECT TRIM(keyword) AS keyword
            FROM keyword_index,
                unnest(string_to_array(artifact_ids, ',')) AS aid
            WHERE aid = %s
        )
        SELECT
            aid2 AS shared_artifact_id,
            TRIM(ki.keyword) AS shared_keyword
        FROM keyword_index ki
        JOIN LATERAL unnest(string_to_array(ki.artifact_ids, ',')) AS aid2 ON TRUE
        JOIN artifact_keywords ak ON TRIM(ki.keyword) = ak.keyword
        WHERE aid2 != %s;
    """, (artifact_id, artifact_id))


    rows = cur.fetchall()
    shared = defaultdict(list)
    for artifact, keyword in rows:
        shared[artifact].append(keyword)
    ## Returns the artifact and keyword that is shared    
    return shared

## Cluster by hashes
def generating_hash_index(cur):
    cur.execute("""
        SELECT jsonb_agg(jsonb_build_object(hash, artifact_ids)) AS hash_artifact_map
        FROM (
            SELECT 
                manifest_entry->>'hash' AS hash,
                ARRAY_AGG(artifact_id) AS artifact_ids
            FROM osc_dataset,
                jsonb_array_elements(data->'public_fields'->'manifest') AS manifest_entry
            GROUP BY hash
            HAVING COUNT(*) > 1
        ) AS sub;
    """)
    
    hashes = cur.fetchone()[0]  # This is a list of dicts like [{"hash1": [1, 2]}, ...]
    hash_artifact_list = json.loads(json.dumps(hashes))  # Make it a Python list of dicts

    for item in hash_artifact_list:
        for h, artifacts in item.items():
            cur.execute("""
                INSERT INTO hash_index (hash, artifact_ids)
                VALUES (%s, %s)
            """, (h, artifacts))
            conn.commit()

## Initializing PSQL table
def init_hash_clusters(artifact_ids, cur):
    visited_artifacts = set()
    
    for(artifact,) in artifact_ids:
        artifact = artifact.strip("{}")
        if artifact in visited_artifacts:
            continue
        edge_list = []
        hash_list = set()
        seen_edges = set()
        recurse_hashes(cur, artifact, edge_list, seen_edges, hash_list, visited_artifacts)
        if(len(edge_list) != 0):
            cur.execute("""
                        INSERT INTO hash_clusters(cluster_hashes, edges)
                        VALUES(%s, %s)
                        """, (",".join(sorted(hash_list)), json.dumps(edge_list)))
        conn.commit()

def recurse_hashes(cur, artifact_id, edge_list, seen_edges, hashes, visited_artifacts):
    artifact_id = artifact_id.strip("{}")  # <-- sanitize
    if artifact_id in visited_artifacts:
        return
    visited_artifacts.add(artifact_id)
    common_hashes = find_artifacts_with_shared_hashes(cur, artifact_id)
    if len(common_hashes) == 0:
        return

    for artifact in common_hashes:
        edge_key = tuple(sorted([artifact_id, artifact]))
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)  # <-- add to avoid infinite loop
        edge_list.append({"node1": artifact_id, "node2": artifact.strip("{}")})
        hashes.update(common_hashes[artifact])
        
        recurse_hashes(cur, artifact, edge_list, seen_edges, hashes, visited_artifacts)


def find_artifacts_with_shared_hashes(cur, artifact_id):
    cur.execute("""
    WITH artifact_hashes AS (
        SELECT TRIM(hash) AS hash
        FROM hash_index,
            unnest(string_to_array(artifact_ids, ',')) AS aid
        WHERE aid = %s
    )
    SELECT
        aid2 AS shared_artifact_id,
        TRIM(ki.hash) AS shared_hash
    FROM hash_index ki
    JOIN LATERAL unnest(string_to_array(ki.artifact_ids, ',')) AS aid2 ON TRUE
    JOIN artifact_hashes ak ON TRIM(ki.hash) = ak.hash
    WHERE aid2 != %s;
""", (artifact_id, artifact_id))

    
    
    rows = cur.fetchall()
    shared = defaultdict(list)
    for artifact, keyword in rows:
        shared[artifact].append(keyword)
    ## Returns the artifact and keyword that is shared    
    return shared

conn = psycopg2.connect(    
    f"host=localhost port=5432 dbname=osc_portal user={os.getenv('DB_USERNAME')} password={os.getenv('DB_PASSWORD')}"
)
cur = conn.cursor()

cur.execute("drop table hash_clusters;")
cur.execute("SELECT artifact_id FROM osc_dataset")
all_artifact_ids = cur.fetchall()

# ## Generate table
cur.execute("""
            CREATE TABLE hash_clusters(
                cluster_hashes TEXT,
                edges TEXT
            );
            """)
conn.commit()

init_hash_clusters(all_artifact_ids, cur)
# init_keyword_cluster(all_artifact_ids, cur)

cur.close()
conn.close()
