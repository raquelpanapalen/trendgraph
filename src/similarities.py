import csv
from tqdm import tqdm
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
from graphdatascience import GraphDataScience


"""
In this script we want to:

1. 🔄 Extract abstract fields from Paper nodes
2. 🧠 Embed them using sentence-transformers
3. 💾 Save embeddings into Neo4j
4. 🧠 Trigger Neo4j GDS to compute pairwise cosine similarities
"""

# ---------- CONFIG ---------- #
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "trendgraph")  # adjust your password
MODEL_NAME = "all-MiniLM-L6-v2"
# ---------------------------- #

driver = GraphDatabase.driver(URI, auth=AUTH)
model = SentenceTransformer(MODEL_NAME)
gds = GraphDataScience(driver)


def get_paper_abstracts():
    with driver.session() as session:
        result = session.run(
            """
            MATCH (p:Paper)
            WHERE p.abstract IS NOT NULL
            RETURN p.paper_id AS id, p.abstract AS abstract
        """
        )
        return [{"id": r["id"], "abstract": r["abstract"]} for r in result]


def store_embeddings(papers):
    with driver.session() as session:
        for paper in tqdm(papers, desc="Storing embeddings"):
            session.run(
                f"""
                MATCH (p:Paper {{paper_id: $id}})
                SET p.embedding = $embedding
            """,
                {"id": paper["id"], "embedding": paper["embedding"]},
            )


def embed_papers(papers):
    texts = [p["abstract"] for p in papers]
    embeddings = model.encode(texts, show_progress_bar=True, normalize_embeddings=True)
    for i, emb in enumerate(embeddings):
        papers[i]["embedding"] = emb.tolist()
    return papers


def create_named_graph():
    """
    Project the graph into a named graph for GDS algorithms with all the Paper nodes (embedding property).
    """

    gds.graph.project.cypher(
        "paper_graph",
        """
        MATCH (p:Paper)
        WHERE p.embedding IS NOT NULL
        RETURN id(p) AS id, labels(p) AS labels, p.embedding AS embedding
        """,
        """
        MATCH (p1:Paper)-[r]->(p2:Paper)
        WHERE p1.embedding IS NOT NULL AND p2.embedding IS NOT NULL
        RETURN id(p1) AS source, id(p2) AS target, type(r) AS type
        """,
    )
    paper_graph = gds.graph.get("paper_graph")
    print(
        f"projected graph: {paper_graph.node_count()} nodes, {paper_graph.relationship_count()} edges"
    )
    print("Named graph 'paper_graph' created successfully.")


def run_gds_node_similarity():
    with driver.session() as session:
        result = session.run(
            """
                CALL gds.knn.write('paper_graph', {
                nodeLabels: ['Paper'],
                nodeProperties: {embedding: 'COSINE'},
                writeRelationshipType: 'SIMILAR_TO',
                writeProperty: 'score',
                topK: 10
                })
                YIELD nodesCompared, relationshipsWritten
            """
        )
        stats = result.single()
        print(
            f"Compared {stats['nodesCompared']} nodes and wrote {stats['relationshipsWritten']} SIMILAR_TO relationships."
        )


def export_similar_to_csv(output_path):
    query = """
        MATCH (p1:Paper)-[r:SIMILAR_TO]->(p2:Paper)
        RETURN p1.title AS source_title, p2.title AS target_title, r.score AS similarity
    """
    with driver.session() as session:
        results = session.run(query)
        rows = [
            (r["source_title"], r["target_title"], r["similarity"]) for r in results
        ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["source_title", "target_title", "similarity"])
        writer.writerows(rows)

    print(f"Exported {len(rows)} rows to {output_path}")


def delete_named_graph():
    with driver.session() as session:
        session.run(
            """
            CALL gds.graph.drop('paper_graph')
            YIELD graphName
            RETURN graphName
            """
        )
        print("Named graph 'paper_graph' deleted successfully.")


def main():
    print("Fetching abstracts...")
    papers = get_paper_abstracts()
    print(f"Found {len(papers)} papers without embeddings.")

    if not papers:
        print("No papers to embed. Exiting.")
        return

    print("Generating embeddings...")
    papers = embed_papers(papers)

    print("Saving embeddings to Neo4j...")
    store_embeddings(papers)

    print("Creating named graph...")
    create_named_graph()

    print("Running GDS node similarity...")
    run_gds_node_similarity()

    print("Exporting similar papers to CSV...")
    export_similar_to_csv("output/similar_papers.csv")

    print("Deleting named graph...")
    delete_named_graph()
    print("Done!")


if __name__ == "__main__":
    main()
