import requests
import json
import time
import datetime
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
    MAX_REQUESTS_PER_DAY = 100000
    REQUEST_COUNT = 0  # Track requests
    FIRST_REQUEST_TIME = None  # Timestamp of the first request

    def __init__(self):
        """Initializes the PaperRetriever."""
        self.data = {
            "works": [],
            "authors": [],
            "citations": [],
            "related_work": [],
            "writes_work": [],
        }

        self.authors_seen = set()
        self.papers_seen = set()

    def _get_seconds_until_next_window(self):
        """Returns the number of seconds until 24 hours after the first request."""
        if not self.FIRST_REQUEST_TIME:
            return 0  # No waiting needed yet

        elapsed_time = (
            datetime.datetime.now() - self.FIRST_REQUEST_TIME
        ).total_seconds()
        return max(0, 86400 - elapsed_time)  # 86400 seconds in a day

    def _rate_limit(self):
        """Ensures compliance with the 100K requests/day limit."""
        if self.FIRST_REQUEST_TIME is None:
            self.FIRST_REQUEST_TIME = datetime.datetime.now()

        self.REQUEST_COUNT += 1

        if self.REQUEST_COUNT >= self.MAX_REQUESTS_PER_DAY:
            sleep_time = self._get_seconds_until_next_window()
            print(
                f"⚠️ Request limit ({self.MAX_REQUESTS_PER_DAY}) reached. Sleeping for {sleep_time / 3600:.2f} hours."
            )
            time.sleep(sleep_time)  # Sleep until 24 hours after the first request
            self.REQUEST_COUNT = 0  # Reset counter
            self.FIRST_REQUEST_TIME = datetime.datetime.now()  # Start new 24h window

        if self.REQUEST_COUNT % 1000 == 0:
            print(f"📊 {self.REQUEST_COUNT} requests sent so far.")

        # time.sleep(self.SLEEP_TIME)  # Control rate

    def fetch_papers(self):
        """Fetches all papers using cursor-based pagination."""
        cursor = "*"
        total_fetched = 0

        while True:
            params = {
                "cursor": cursor,
                "filter": self.FILTER_QUERY,
                "per_page": self.PER_PAGE,
            }
            response = requests.get(self.OPENALEX_URL, params=params)
            self._rate_limit()  # Ensure compliance

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
                # time.sleep(self.SLEEP_TIME)  # Avoid API rate limits
            else:
                print(f"⚠️ OpenAlex API error: {response.status_code}")
                break

        print(f"🎯 Finished fetching. Total papers processed: {total_fetched}")

    def _get_citations_openalex(self, paper):
        if paper.get("cited_by_api_url", None):
            response = requests.get(paper.get("cited_by_api_url", None))
            if response.status_code == 200:
                data = response.json()
                return data.get("results", [])
        return []

    def process_paper(self, paper):
        """Extracts and processes nodes and.data from a paper."""
        paper_id = paper["id"]

        if paper_id in self.papers_seen:
            return  # Skip duplicates

        # Add work node
        self.papers_seen.add(paper_id)
        self.data["works"].append(format_paper(paper))

        # Process authors
        for author in paper.get("authorships", []):
            author_id = author.get("author", {}).get("id")
            author_name = author.get("author", {}).get("display_name")

            if not author_id:
                continue

            if author_id not in self.authors_seen:
                self.data["authors"].append({"id": author_id, "name": author_name})
                self.authors_seen.add(author_id)

            self.data["writes_work"].append(
                {"author_id": author_id, "paper_id": paper_id}
            )

        # Process citations (papers citing this work)
        for citing_paper in self._get_citations_openalex(paper):
            citing_paper_id = citing_paper.get("id")

            # Check if citing paper is already processed
            if citing_paper_id in self.papers_seen:
                self.data["citations"].append({"from": paper_id, "to": citing_paper_id})

        # Process related work
        for referenced_paper in paper.get("related_works", []):
            response = requests.get(
                f"{self.OPENALEX_URL}/{referenced_paper.split('/')[-1]}"
            )
            self._rate_limit()  # Ensure compliance
            if response.status_code == 200:
                ref_paper = response.json()
                ref_paper_id = ref_paper.get("id")

                self.data["related_work"].append({"from": paper_id, "to": ref_paper_id})

                if ref_paper_id not in self.papers_seen:
                    self.papers_seen.add(ref_paper_id)
                    self.data["works"].append(format_paper(ref_paper))

    def save_to_json(self, filename):
        """Saves the extracted nodes and.data to a JSON file."""
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=4)

        print(
            f"Number of nodes: {len(self.data['works'])} works + {len(self.data['authors'])} authors"
        )
        print(
            f"Number of edges: {len(self.data['citations'])} citations + {len(self.data['related_work'])} related works + {len(self.data['writes_work'])} writes works"
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
