from neo4j import GraphDatabase
from graphdatascience import GraphDataScience
import pandas as pd
from collections import Counter
from sklearn.metrics import normalized_mutual_info_score

# Neo4j connection
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "trendgraph")
driver = GraphDatabase.driver(URI, auth=AUTH)
gds = GraphDataScience(driver)


POPULAR_TOPICS = [
    "Advanced Neural Network Applications",
    "Advanced Vision and Imaging",
    "Advanced Image and Video Retrieval Techniques",
    "Domain Adaptation and Few-Shot Learning",
    "Multimodal Machine Learning Applications",
    "Human Pose and Action Recognition",
    "Video Surveillance and Tracking Methods",
    "Advanced Image Processing Techniques",
    "Anomaly Detection Techniques and Applications",
    "Topic Modeling",
]

POPULAR_TOPIC_SET = set(POPULAR_TOPICS)


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


def run_gds_kmeans():
    with driver.session() as session:
        result = session.run(
            """
                CALL gds.kmeans.write('paper_graph', {
                    nodeLabels: ['Paper'],
                    nodeProperty: 'embedding',
                    writeProperty: 'cluster',
                    k: 10,
                    maxIterations: 20
                })
            """
        )


def fetch_clusters_and_topics():
    with driver.session() as session:
        result = session.run(
            """
            MATCH (p:Paper)-[:HAS_TOPIC]->(t:Topic)
            RETURN p.paper_id AS id, p.cluster AS cluster, t.name AS topic
        """
        )
        return [
            {"id": r["id"], "cluster": r["cluster"], "topic": r["topic"]}
            for r in result
        ]


def compute_topic_distribution(data, output_csv_path):
    df = pd.DataFrame(data)

    # Compute topic counts per cluster
    cluster_topic_counts = (
        df.groupby(["cluster", "topic"])
        .size()
        .reset_index(name="count")
        .query("count > 100")
        .sort_values(["cluster", "count"], ascending=[True, False])
    )

    # Export to CSV
    cluster_topic_counts.to_csv(output_csv_path, index=False)
    print(f"Exported topic distribution to {output_csv_path}")


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
    delete_named_graph()
    print("Creating named graph...")
    create_named_graph()

    print("Running KMeans...")
    run_gds_kmeans()

    print("Fetching paper cluster/topic assignments...")
    data = fetch_clusters_and_topics()

    print("Computing and exporting topic distribution...")
    compute_topic_distribution(data, "output/topic_distribution_per_cluster.csv")

    print("Deleting named graph...")
    delete_named_graph()
    print("Done!")


if __name__ == "__main__":
    main()
