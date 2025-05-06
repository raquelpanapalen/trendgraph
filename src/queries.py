import os
import csv
from neo4j import GraphDatabase, Session


def save_to_csv(filename, data):
    with open(filename, "w", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


def most_popular_topics(tx: Session):
    """
    Returns the most popular topics based on the number of papers associated with each topic.
    """
    query = """
        MATCH (:Paper)-[:HAS_TOPIC]->(t:Topic)
        RETURN t.name AS topic, count(*) AS paper_count
        ORDER BY paper_count DESC
        LIMIT 10
    """
    result = tx.run(query)
    return [
        {"topic": record["topic"], "paper_count": record["paper_count"]}
        for record in result
    ]


def get_emerging_topics(tx: Session):
    """
    Returns the emerging topics based on the number of papers associated with each topic.
    """
    query = """
        MATCH (p:Paper)-[:HAS_TOPIC]->(t:Topic)
        WHERE p.year IS NOT NULL
        RETURN t.name AS topic, p.year AS year, count(*) AS papers_published
        ORDER BY year ASC, papers_published DESC
    """
    result = tx.run(query)
    return [
        {
            "topic": record["topic"],
            "year": record["year"],
            "papers_published": record["papers_published"],
        }
        for record in result
    ]


def most_influential_topics(tx: Session):
    """
    Returns the most influential topics based on the number of citations received by papers associated with each topic.
    """
    query = """
        MATCH (p:Paper)-[:HAS_TOPIC]->(t:Topic)
        WHERE p.citations IS NOT NULL
        RETURN t.name AS topic, sum(p.citations) AS total_citations
        ORDER BY total_citations DESC
        LIMIT 10
    """
    result = tx.run(query)
    return [
        {"topic": record["topic"], "total_citations": record["total_citations"]}
        for record in result
    ]


def most_influential_authors_by_topic(tx: Session):
    """
    Returns the most influential authors by topic based on the number of citations received by papers associated with each topic.
    """
    query = """
        MATCH (a:Author)-[:WROTE]->(p:Paper)-[:HAS_TOPIC]->(t:Topic)
        WHERE p.citations IS NOT NULL
        RETURN a.name AS author, t.name AS topic, sum(p.citations) AS total_citations
        ORDER BY total_citations DESC
        LIMIT 50
    """

    result = tx.run(query)
    return [
        {
            "topic": record["topic"],
            "author": record["author"],
            "total_citations": record["total_citations"],
        }
        for record in result
    ]


def top_authors_involved_in_similar_papers(tx: Session):
    """
    Identify which authors are most commonly co-involved in similar works.
    """
    query = """
        MATCH (a1:Author)-[:WROTE]->(p1:Paper)-[:SIMILAR_TO]->(p2:Paper)<-[:WROTE]-(a2:Author)
        WHERE a1 <> a2 AND a1.id < a2.id
        WITH a1, a2, count(*) AS similarity_score
        ORDER BY similarity_score DESC
        RETURN a1.name AS author1, a2.name AS author2, similarity_score
        LIMIT 20
    """

    result = tx.run(query)
    return [
        {
            "author1": record["author1"],
            "author2": record["author2"],
            "similarity_score": record["similarity_score"],
        }
        for record in result
    ]


def link_similar_authors(tx: Session):
    """
    Create relationships between authors who have similar papers.
    """
    query = """
        MATCH (a1:Author)-[:WROTE]->(p1:Paper)-[:SIMILAR_TO]->(p2:Paper)<-[:WROTE]-(a2:Author)
        WHERE a1 <> a2 AND a1.id < a2.id
        WITH a1, a2, count(*) AS similarity_count
        WHERE similarity_count > 10
        MERGE (a1)-[r:SIMILAR_AUTHOR]->(a2)
        SET r.similarity_count = similarity_count
    """

    tx.run(query)


if __name__ == "__main__":
    # Initialize Neo4j connection
    URI = "bolt://localhost:7687"
    AUTH = ("neo4j", "trendgraph")

    output_dir = "output/"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Build the knowledge graph
    with GraphDatabase.driver(URI, auth=AUTH).session() as session:
        # Get the most popular topics
        result = session.execute_read(most_popular_topics)
        save_to_csv(os.path.join(output_dir, "most_popular_topics.csv"), result)

        # Get the emerging topics
        result = session.execute_read(get_emerging_topics)
        save_to_csv(os.path.join(output_dir, "emerging_topics.csv"), result)

        # Get the most influential topics
        result = session.execute_read(most_influential_topics)
        save_to_csv(os.path.join(output_dir, "most_influential_topics.csv"), result)

        # Get the most influential authors by topic
        result = session.execute_read(most_influential_authors_by_topic)
        save_to_csv(
            os.path.join(output_dir, "most_influential_authors_by_topic.csv"), result
        )

        # Get the top authors involved in similar papers
        result = session.execute_read(top_authors_involved_in_similar_papers)
        save_to_csv(
            os.path.join(output_dir, "top_authors_involved_in_similar_papers.csv"),
            result,
        )

        # Link similar authors
        session.execute_write(link_similar_authors)

        print("Results saved to CSV files.")
