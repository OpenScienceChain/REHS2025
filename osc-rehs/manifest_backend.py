from flask import Flask, jsonify, request
from dotenv import load_dotenv

from flask_cors import CORS
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from collections import defaultdict 
import itertools
from itertools import combinations
import os

app = Flask(__name__)

CORS(app, resources={
    r"/*": {"origins": "http://localhost:8000"}
})

load_dotenv()
db_pool = psycopg2.pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    host="localhost",
    port=5432,
    dbname="osc_portal",
    user=os.getenv('DB_USERNAME'),
    password=os.getenv('DB_PASSWORD')
)

@contextmanager
def get_db_cursor():
    conn = db_pool.getconn()
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
        cur.close()
    finally:
        db_pool.putconn(conn)
#region
@app.route('/manifest/<specific_artifact_id>/', methods=['GET'])
def manifest(specific_artifact_id):
    try:            
        with get_db_cursor() as cur:            
            cur.execute("SELECT artifact_id, data FROM osc_dataset")
            records = cur.fetchall()
            
            result = []
            hash_and_files = defaultdict(set)
            hash_to_artifacts = defaultdict(set)
            edges = defaultdict(set)
            for artifact_id, data in records:
                hashes = []         
                manifest = data.get("public_fields", {}).get("manifest", {})
                title = data.get("mandatory_public_fields",{}).get("title", "No Title")

                for item in manifest:
                    hashes.append(item.get("hash")) 
                    hash_and_files[item.get("hash")].add(item.get("filename"))
                    hash = item.get("hash")
                    hash_to_artifacts[hash].add(artifact_id)
                hash_and_files_serializable = {k: list(v) for k, v in hash_and_files.items()}
                
                result.append({
                    "artifact_id": artifact_id,
                    "title": title,
                    "hashes": hashes,
                    "hash_and_files": hash_and_files_serializable
                })
            
            for h, artifact_ids in hash_to_artifacts.items():
                if len(artifact_ids) > 1:
                    for artifact_id in artifact_ids:
                        edges[h].add(artifact_id)
            
            final_edges = []
            
            for h, artifact_ids in edges.items():
                for node1, node2 in itertools.combinations(artifact_ids, 2):
                    final_edges.append({
                        "node1": node1,
                        "node2": node2,
                        "hash": h
                    })
            
            # Convert defaultdict to a regular dict for JSON serialization

            
            specific_nodes = set()
            if specific_artifact_id != "all":
                filtered_edges = []
                specific_nodes = set()
                for edge in final_edges:
                    if edge["node1"] == specific_artifact_id or edge["node2"] == specific_artifact_id:
                        filtered_edges.append(edge)
                        specific_nodes.add(edge["node1"])
                        specific_nodes.add(edge["node2"])
                final_edges = filtered_edges

                if len(final_edges) > 0:
                    result = [node for node in result if node["artifact_id"] in specific_nodes]
                else:
                    result = [node for node in result if node["artifact_id"] == specific_artifact_id]
            result.append(
            {
                "edges": final_edges
            }
            )
            return {"manifest": result}
    
    except Exception as e:
        return {"error": str(e)}, 500
#endregion
## Getting specific artifact data
@app.route('/artifact/<artifact_id>/', methods=['GET'])
def get_artifact(artifact_id):
    try:
        with get_db_cursor() as cur:        
            cur.execute("SELECT data FROM osc_dataset WHERE artifact_id = %s;", (artifact_id,))
            row = cur.fetchone()
            data = row[0]
            hash_and_files = defaultdict(set)

            manifest = data.get("public_fields", {}).get("manifest", {})

            for item in manifest:
                hash_and_files[item.get("hash")].add(item.get("filename"))
                hash_and_files_serializable = {k: list(v) for k, v in hash_and_files.items()}
            
            
            return {
                "artifact_id": artifact_id,
                "title": data.get("mandatory_public_fields", {}).get("title", "No Title"),
                "keywords": data.get("public_fields", {}).get("keywords", []),
                "manifest": hash_and_files_serializable,
                "doi": data.get("public_fields", {}).get("doi", {}),
                "contributor": data.get("public_fields", {}).get("contributor", "No Contributor"),
                    }
            
    except Exception as e:
        return {"error": str(e)}, 500
    

@app.route('/cluster/contributor', methods=['GET'])
def get_contributor_cluster_names():
    with get_db_cursor() as cur:
        cur.execute("SELECT contributor FROM contributor_index;")
        
        data = cur.fetchall()
        contributor_names = [contributor[0] for contributor in data]
        contributor_data = []
        for contributor in contributor_names:
            cur.execute("SELECT artifact_ids FROM contributor_index WHERE TRIM(contributor) ILIKE %s", (contributor.strip(),))
            data = cur.fetchall()
            row = [i[0] for i in data]
            artifacts = [a.strip() for a in row[0].split(',')] if row and row[0] else []
            contributor_data.append({"contributor": contributor, "num_artifacts": len(artifacts)})

        return {"contributors": contributor_data}

@app.route('/cluster/contributor/<path:contributor>', methods=['GET'])
def get_contributor_cluster_values(contributor):
    with get_db_cursor() as cur:
        cur.execute( "SELECT artifact_ids FROM contributor_index WHERE TRIM(contributor) ILIKE %s",
            (contributor.strip(),))

        data = cur.fetchall()
        row = [i[0] for i in data]
        artifacts = [a.strip() for a in row[0].split(',')] if row and row[0] else []
        if(len(artifacts) > 1):
            edges = [
                {"node1": a, "node2": b}
                for a, b in combinations(artifacts, 2)
            ]
        else:
            edges = [{"node1": artifacts[0]}]

        return {"edges": edges}

## Keyword Clustering
@app.route('/cluster/keywords', methods=['GET'])
def get_keyword_cluster_names():
    with get_db_cursor() as cur:
        cur.execute("SELECT cluster_name, jsonb_array_length(edges::jsonb) AS edge_count FROM keyword_clusters;")
        keywords = cur.fetchall()
        keyword_data = [{"cluster_name": kw[0], "edge_count": kw[1]} for kw in keywords]

        

        return {"keywords" : keyword_data}

@app.route('/cluster/keywords/<path:cluster_name>', methods=['GET'])
def get_keyword_cluster_values(cluster_name):
    with get_db_cursor() as cur:
        cur.execute("SELECT edges FROM keyword_clusters WHERE cluster_name=%s;",(cluster_name,))
        data = cur.fetchall()
        edges = [i[0] for i in data]


        return {"edges": edges}

@app.route('/cluster/hashes/', methods=["GET"])
def get_hash_cluster_names():
    with get_db_cursor() as cur:
        cur.execute("SELECT cluster_hashes, jsonb_array_length(edges::jsonb) AS edge_count FROM hash_clusters;")
        clusters = cur.fetchall()
        cluster_data = [{"cluster_name": i[0], "edge_count": i[1]} for i in clusters]
    
    return {"hashes": cluster_data}

@app.route('/cluster/hashes/<path:cluster_name>', methods=['GET'])
def get_hash_cluster_values(cluster_name):
    with get_db_cursor() as cur:
        cur.execute("SELECT edges FROM hash_clusters WHERE cluster_hashes=%s;",(cluster_name,))
        data = cur.fetchall()
        edges = [i[0] for i in data]

    return {"edges": edges}

## Getting the artifacts that have same keywords
@app.route('/artifact/keywords/<artifact_id>/', methods=['GET'])
def get_all_artifacts_w_keywords(artifact_id):
    with get_db_cursor as cur:
        seen_edges = set()
        edge_list = []
        depth = 5
        recurse_keywords(cur, artifact_id, edge_list, seen_edges, depth, depth_counter=0)

    return jsonify({"edge_list":edge_list})

def recurse_keywords(cur, artifact_id, edges, seen_edges, max_depth, depth_counter):
    ## Checks if visited the artifact_id already
    if depth_counter >= max_depth:
        return
    
    ## Checking the nodes that are immediately connected with the keyword
    common_artifacts = find_artifacts_with_shared_keywords(cur, artifact_id)
    if len(common_artifacts) == 0:
        return
    
    ## If there was a repeat edge it continues to loop through. 
    for artifact in common_artifacts:
        edge_key = tuple(sorted([artifact_id, artifact]))
        if edge_key in seen_edges:
            continue
        edges.append({"node1": artifact_id, "node2": artifact, "shared_keywords": common_artifacts[artifact], "node1depth": depth_counter,"node2depth": depth_counter+1})
        seen_edges.add(edge_key)
        recurse_keywords(cur, artifact, edges, seen_edges, max_depth, depth_counter + 1)

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
    
    
@app.route('/artifact/contributor/<artifact_id>/', methods=['GET'])
def get_all_artifacts_w_contributor(artifact_id):
    with get_db_cursor() as cur:
        artifacts, contributor = find_artifact_with_shared_contributor(cur, artifact_id)
        edge_list = []
        for artifact in artifacts:
            edge_list.append({"node1": artifact_id, "node2": artifact, "shared_contributor": contributor})
        
    return jsonify({"edge_list": edge_list})

def find_artifact_with_shared_contributor(cur, artifact_id):
    cur.execute(f"SELECT contributor FROM contributor_index where artifact_ids LIKE '%{artifact_id}%';")
    contributor = cur.fetchone()
    contributor = contributor[0]
    
    cur.execute("SELECT artifact_ids FROM contributor_index WHERE contributor = %s;", (contributor,))
    row = cur.fetchone()
    artifact_ids = row[0].split(',') if row and row[0] else []
    artifact_ids.remove(artifact_id)
    
    return artifact_ids, contributor

#region Get data from a specific artifact
def get_artifact_data(artifact_id):
    with get_db_cursor() as cur:
    
        cur.execute(f"SELECT data FROM osc_dataset WHERE artifact_id = {artifact_id}")
        record = cur.fetchall()
        
        data = record[0]
        
        artifact_data = {}
        artifact_data["artifact_id"] = artifact_id
        artifact_data["title"] = data.get("mandatory_public_fields", {}).get("title", "No Title")
        artifact_data["keywords"] = data.get("public_fields", {}).get("keywords", [])
        artifact_data["manifest"] = data.get("public_fields", {}).get("manifest", {})
        artifact_data["doi"] = data.get("public_fields", {}).get("doi", {})
        artifact_data["contributor"] = data.get("public_fields", {}).get("contributor", "No Contributor")

    return artifact_data
#endregion

## Get Edge Data
@app.route('/edge/<path:node1>/<path:node2>/<edge_type>', methods=['GET'])
def get_edge_data(node1, node2, edge_type):

    if edge_type not in ['hash', 'keyword', 'contributor']:
        return jsonify({"error": "Invalid edge type. Must be 'hash', 'keyword', or 'contributor'"}), 400
    
    with get_db_cursor() as cur:
        if edge_type == 'contributor':
            cur.execute("SELECT contributor FROM contributor_index WHERE artifact_ids LIKE %s;", (f"%{node1}%",))
            row = cur.fetchone()
            if row:
                return jsonify({"contributor": row[0]})
            else:
                return jsonify
        elif edge_type == 'keyword':
            cur.execute("""
                SELECT keyword FROM keyword_index
                WHERE artifact_ids LIKE %s AND artifact_ids LIKE %s;
            """, (f"%{node1}%", f"%{node2}%"))
            row = cur.fetchall()
            ##flatten row
            row = [i[0] for i in row]
            
            if row:
                return jsonify({"keyword": row}), 200
            else:
                return jsonify({"error": "No keyword found"}), 404

        elif edge_type == 'hash':
            cur.execute("""
                SELECT hash FROM hash_index
                WHERE artifact_ids LIKE %s AND artifact_ids LIKE %s;
            """, (f"%{node1}%", f"%{node2}%"))
            row = cur.fetchall()
            row = [i[0] for i in row]
            
            if row:
                return jsonify({"hash": row}), 200
            else:
                return jsonify({"error": "No hash found"}), 404


@app.route('/filehash/<path:hash_value>/', methods=['GET'])
def get_filehash_data(hash_value):
    try:
        with get_db_cursor() as cur:
        
            cur.execute("""
            SELECT 
            m->>'filename' AS filename
            FROM osc_dataset,
            jsonb_array_elements(data->'public_fields'->'manifest') AS m
            WHERE m->>'hash' = %s;
            """, (hash_value,))
            
            records = cur.fetchone()

            
            filename = records[0] if records else None
            return jsonify(filename)
    
    except Exception as e:
        return {"error": str(e)}, 500
if __name__ == '__main__':
    app.run(port=5000)