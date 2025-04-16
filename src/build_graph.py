import json
from tqdm import tqdm
from neo4j import GraphDatabase, Session


def create_graphdb(tx: Session, data):
    # Create authors
    for author in tqdm(data["authors"]):
        tx.run("MERGE (a:Author {id: $id})", id=author["id"])

    # Create works
    for work in tqdm(data["works"]):
        tx.execute_write(
            lambda tx, elem: tx.run(
                """
                    MERGE (p:Paper {paper_id: $paper_id})
                    ON CREATE SET p.title = $title, p.abstract = $abstract, p.year = $year, p.citations = $citations
                """,
                elem,
            ),
            work,
        )

        for topic in work["topics"]:
            topic_data = {"paper_id": work["paper_id"], "topic_name": topic}

            tx.execute_write(
                lambda tx, elem: tx.run(
                    """
                        MERGE (t:Topic {name: $topic_name})
                        WITH t
                        MATCH (p:Paper {paper_id: $paper_id})
                        MERGE (p)-[:HAS_TOPIC]->(t)
                    """,
                    topic_data,
                ),
                topic_data,
            )

    # Create WROTE relationships (Author → Paper)
    for elem in tqdm(data["writes_work"]):
        tx.execute_write(
            lambda tx, elem: tx.run(
                """
                MATCH (a:Author {author_id: $author_id})
                MATCH (p:Paper {paper_id: $paper_id})
                MERGE (a)-[:WROTE]->(p)
            """,
                elem,
            ),
            elem,
        )

    # Create CITED relationships (Paper → Paper)
    for elem in tqdm(data["citations"]):
        tx.execute_write(
            lambda tx, elem: tx.run(
                """
                MATCH (src:Paper {paper_id: $from})
                MATCH (tgt:Paper {paper_id: $to})
                MERGE (src)-[:CITES]->(tgt)
            """,
                elem,
            ),
            elem,
        )

    # Create RELATED relationships (Paper → Paper)
    for elem in tqdm(data["related_work"]):
        tx.execute_write(
            lambda tx, elem: tx.run(
                """
                MATCH (src:Paper {paper_id: $from})
                MATCH (tgt:Paper {paper_id: $to})
                MERGE (src)-[:RELATED]->(tgt)
            """,
                elem,
            ),
            elem,
        )


if __name__ == "__main__":
    with open("data/openalex_research_papers.json", "r") as f:
        openalex_data = json.load(f)

    # Initialize Neo4j connection
    URI = "bolt://localhost:7687"
    AUTH = ("neo4j", "trendgraph")

    # Build the knowledge graph
    with GraphDatabase.driver(URI, auth=AUTH).session() as session:
        session.execute_write(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
        create_graphdb(session, openalex_data)
        print("Graph database initialized.")
