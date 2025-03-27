import requests
import json
import time
from tqdm import tqdm

from src.openalex import format_paper


class PaperRetriever:
    """
    A class to retrieve academic papers from OpenAlex and Semantic Scholar
    based on AI research topics.
    """

    OPENALEX_URL = "https://api.openalex.org/works"
    FILTER_QUERY = "primary_location.source.id:s2485537415|s4306512817|s4210176548|s4363607748|s4363607701,primary_topic.subfield.id:subfields/1702|subfields/1707"
    PER_PAGE = 100
    SLEEP_TIME = 1  # To avoid rate limits

    def __init__(self):
        """Initializes the PaperRetriever."""
        self.nodes = {"works": [], "authors": []}  # , "topics": []}
        self.edges = {
            # "citations": [],
            "related_work": {},
            "writes_work": {},
            # "same_topic": [],
        }
        self.authors_seen = set()
        # self.topics_seen = set()
        self.papers_seen = set()
        # self.topic_to_papers = {}  # Maps topic_id -> list of paper_ids

    def fetch_papers(self):
        """Fetches all papers using cursor-based pagination."""
        cursor = "*"
        total_fetched = 0
        i = 0

        while i != 3:
            params = {
                "cursor": cursor,
                "filter": self.FILTER_QUERY,
                "per_page": self.PER_PAGE,
            }
            response = requests.get(self.OPENALEX_URL, params=params)

            if response.status_code == 200:
                data = response.json()
                papers = data.get("results", [])
                cursor = data.get("meta", {}).get(
                    "next_cursor"
                )  # Get next cursor value

                if not papers:
                    break  # No more results

                for paper in papers:
                    self.process_paper(paper)
                    total_fetched += 1

                print(f"✅ Processed {total_fetched} papers so far...")
                time.sleep(self.SLEEP_TIME)  # Avoid API rate limits
                i += 1
            else:
                print(f"⚠️ OpenAlex API error: {response.status_code}")
                break

        print(f"🎯 Finished fetching. Total papers processed: {total_fetched}")

        # Create topic-based links after processing all papers
        # self.create_topic_links()

    def _get_citations_openalex(self, paper):
        if paper.get("cited_by_api_url", None):
            response = requests.get(paper.get("cited_by_api_url", None))
            if response.status_code == 200:
                data = response.json()
                return data.get("results", [])
        return []

    def process_paper(self, paper):
        """Extracts and processes nodes and edges from a paper."""
        paper_id = paper["id"]

        if paper_id in self.papers_seen:
            return  # Skip duplicates
        self.papers_seen.add(paper_id)

        # Add work node
        self.nodes["works"].append(format_paper(paper))

        """# Add topic node & track paper under its topic
        topics = paper.get("topics", [])
        for topic in topics:
            topic_id = topic.get("id")
            topic_name = topic.get("display_name")
            if topic_id not in self.topics_seen:
                self.nodes["topics"].append({"id": topic_id, "name": topic_name})
                self.topics_seen.add(topic_id)

            # Store paper under its topic
            if topic_id not in self.topic_to_papers:
                self.topic_to_papers[topic_id] = []
            self.topic_to_papers[topic_id].append(paper_id)"""

        # Process authors
        for author in paper.get("authorships", []):
            author_id = author.get("author", {}).get("id")
            author_name = author.get("author", {}).get("display_name")

            if not author_id:
                continue

            if author_id not in self.authors_seen:
                self.nodes["authors"].append({"id": author_id, "name": author_name})
                self.authors_seen.add(author_id)

            if author_id not in self.edges["writes_work"]:
                self.edges["writes_work"][author_id] = [paper_id]
            else:
                self.edges["writes_work"][author_id].append(paper_id)

        # Process citations (papers citing this work)
        """for citing_paper in self._get_citations_openalex(paper):
            citing_paper_id = citing_paper.get("id")
            self.edges["citations"].append(
                {"source": citing_paper_id, "target": paper_id}
            )
            # Check if citing paper is already processed
            if citing_paper_id not in self.papers_seen:
                self.papers_seen.add(citing_paper_id)
                self.nodes["works"].append(format_paper(citing_paper))"""

        # Process related work
        for referenced_paper in paper.get("related_works", []):
            response = requests.get(
                f"{self.OPENALEX_URL}/{referenced_paper.split('/')[-1]}"
            )
            if response.status_code == 200:
                ref_paper = response.json()
                ref_paper_id = ref_paper.get("id")

                if paper_id not in self.edges["related_work"]:
                    self.edges["related_work"][paper_id] = [ref_paper_id]
                else:
                    self.edges["related_work"][paper_id].append(ref_paper_id)

                if ref_paper_id not in self.papers_seen:
                    self.papers_seen.add(ref_paper_id)
                    self.nodes["works"].append(format_paper(ref_paper))

    def create_topic_links(self):
        """Creates edges between papers that share the same topic."""
        for topic_id, paper_list in self.topic_to_papers.items():
            num_papers = len(paper_list)
            if num_papers > 1:
                for i in range(num_papers):
                    for j in range(i + 1, num_papers):
                        self.edges["same_topic"].append(
                            {
                                "source": paper_list[i],
                                "target": paper_list[j],
                                "relation": "same_topic",
                            }
                        )
        print(f"🔗 Created {len(self.edges['same_topic'])} same-topic edges.")

    def save_to_json(self, filename):
        """Saves the extracted nodes and edges to a JSON file."""
        with open(filename, "w", encoding="utf-8") as f:
            json.dump({"nodes": self.nodes, "edges": self.edges}, f, indent=4)

        print(
            f"Number of nodes: {len(self.nodes['works'])} works + {len(self.nodes['authors'])} authors"
        )
        print(
            "Number of edges:",
            len(self.edges["citations"]) + len(self.edges["references"]),
        )

        print(f"✅ Graph saved to {filename}")

    def run(self, output_file="ai_research_papers.json"):
        """Runs the full pipeline: fetching and saving papers."""
        self.fetch_papers()
        self.save_to_json(filename=output_file)


if __name__ == "__main__":
    graph = PaperRetriever()
    graph.fetch_papers()
    graph.save_to_json()
    exit()
