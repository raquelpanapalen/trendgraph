import requests
import json
import time
import uuid
from tqdm import tqdm

from src.config import OPENALEX_URL, SEMANTIC_SCHOLAR_URL, TOPICS
from src.openalex import format_paper


class PaperRetriever:
    """
    A class to retrieve academic papers from OpenAlex and Semantic Scholar
    based on AI research topics.
    """

    def __init__(self, max_results=100, sleep_time=1):
        """
        Initializes the PaperRetriever.

        :param topics: Dictionary of research topics categorized as emerging, established, and declining.
        :param max_results: Maximum number of papers to fetch per topic per source.
        :param sleep_time: Time (in seconds) to wait between API calls to avoid rate limits.
        """

        self.OPENALEX_URL = OPENALEX_URL
        self.SEMANTIC_SCHOLAR_URL = SEMANTIC_SCHOLAR_URL
        self.TOPICS = TOPICS

        self.max_results = max_results
        self.sleep_time = sleep_time
        self.all_papers = []
        self.paper_set = dict()  # To track duplicates

    def _is_duplicate(self, paper, source):
        """Checks if a paper with the same title or DOI already exists."""

        if source == "OpenAlex":
            id = f"{paper.get('id', None)}-OpenAlex"
            title = paper.get("title", None)
            doi = paper.get("doi", None)

        elif source == "SemanticScholar":
            id = f"{paper.get('paperId', None)}-SemanticScholar"
            title = paper.get("title", None)
            doi = paper.get("externalIds", {}).get("DOI", None)

        title_key = title.lower().strip() if title else None
        doi_key = doi.lower().strip().replace("https://doi.org/", "") if doi else None

        # Check if the paper is already in the tracker
        if id in self.paper_set:  # Same source (no need to check title and doi)
            return self.paper_set[id]

        # Same paper (merge by title) by different sources
        if title_key in self.paper_set:
            if doi_key and doi_key not in self.paper_set[title_key]:
                self.paper_set[doi_key] = self.paper_set[title_key]

            self.paper_set[id] = self.paper_set[title_key]
            return self.paper_set[title_key]

        # Same paper (merge by doi) by different sources
        if doi_key and doi_key in self.paper_set:
            if title_key not in self.paper_set[doi_key]:
                self.paper_set[title_key] = self.paper_set[doi_key]

            self.paper_set[id] = self.paper_set[doi_key]
            return self.paper_set[doi_key]

        # Completely new paper
        new_id = str(uuid.uuid4())
        self.paper_set[id] = new_id
        self.paper_set[title_key] = new_id
        self.paper_set[doi_key] = new_id
        return new_id

    def _get_citations_openalex(self, paper):
        if paper.get("cited_by_api_url", None):
            response = requests.get(paper.get("cited_by_api_url", None))
            if response.status_code == 200:
                data = response.json()
                return data.get("results", [])
        return []

    def fetch_openalex_papers(self, topic):
        """Fetches papers from OpenAlex API using pagination."""
        results = []
        per_page = (
            self.max_results if self.max_results < 200 else 25
        )  # OpenAlex default
        total_fetched = 0
        page = 1

        while total_fetched < self.max_results:
            params = {
                "search": topic,
                "per_page": per_page,
                "sort": "relevance_score:desc",
                "page": page,
            }
            response = requests.get(self.OPENALEX_URL, params=params)

            if response.status_code == 200:
                data = response.json()
                papers = data.get("results", [])
                page += 1  # Get next page for pagination

                if not papers:
                    break  # No more papers available

                for paper in papers:
                    paper_id = self._is_duplicate(paper, "OpenAlex")

                    # Get citations (other works --> this work)
                    citations = self._get_citations_openalex(paper)
                    cited_by = []
                    for work in citations:
                        work_paper_id = self._is_duplicate(work, "OpenAlex")
                        cited_by.append(work_paper_id)
                        results.append(format_paper(work_paper_id, work, topic))

                    # Get references (this work --> other works)
                    references = paper.get("referenced_works", [])
                    references_to = []
                    for ref_url in references:
                        response = requests.get(
                            f"{self.OPENALEX_URL}/{ref_url.split('/')[-1]}"
                        )
                        if response.status_code == 200:
                            ref = response.json()
                            ref_id = self._is_duplicate(ref, "OpenAlex")
                            references_to.append(ref_id)
                            results.append(format_paper(ref_id, ref, topic))

                    results.append(
                        format_paper(paper_id, paper, topic, cited_by, references_to)
                    )

                    total_fetched += 1
                    if total_fetched >= self.max_results:
                        break  # Stop if max results reached

                time.sleep(self.sleep_time)  # ⏳ Avoid API rate limits
            else:
                print(f"⚠️ OpenAlex API error for {topic}: {response.status_code}")
                break

        return results

    def fetch_semantic_scholar_papers(self, topic):
        """Fetches papers from Semantic Scholar API using pagination."""
        results = []
        per_page = 100  # Semantic Scholar default
        total_fetched = 0
        offset = 0

        while total_fetched < self.max_results:
            params = {
                "query": topic,
                "fields": "paperId,title,authors,year,citationCount,abstract,externalIds,venue",
                "limit": per_page,
                "offset": offset,
            }
            response = requests.get(self.SEMANTIC_SCHOLAR_URL, params=params)

            if response.status_code == 200:
                data = response.json().get("data", [])
                if not data:
                    break  # No more papers available

                for paper in data:
                    semscholar_id = f"{paper.get('paperId', None)}-SemanticScholar"
                    title = paper.get("title", None)
                    doi = paper.get("externalIds", {}).get("DOI", None)
                    paper_id = self._is_duplicate(semscholar_id, title, doi)
                    results.append(
                        {
                            "paper_id": paper_id,
                            "title": title,
                            "doi": doi,
                            "authors": [
                                {
                                    "author_name": author_info.get("name", None),
                                    "institutions": author_info.get("affiliations", []),
                                }
                                for author_info in paper.get("authors", [])
                            ],
                            "year": paper.get("year", None),
                            "citations": paper.get("citationCount", None),
                            "abstract": paper.get("abstract", None),
                            "source": paper.get("venue", None),
                            "topic": topic,
                        }
                    )
                    total_fetched += 1
                    if total_fetched >= self.max_results:
                        break

                offset += per_page
                time.sleep(self.sleep_time)
            else:
                print(
                    f"⚠️ Semantic Scholar API error for {topic}: {response.status_code}"
                )
                print("Try again (it was timeout)")
                # break

        return results

    def collect_papers(self):
        """Retrieves papers for all topics from both sources."""
        for category, topics in self.TOPICS.items():
            for topic in tqdm(topics, desc=f"Fetching {category.upper()} Topics"):
                papers_openalex = self.fetch_openalex_papers(topic)
                # papers_semantic = self.fetch_semantic_scholar_papers(topic)
                # self.all_papers.extend(papers_openalex + papers_semantic)

    def save_to_json(self, filename="ai_research_papers.json"):
        """Saves collected papers to a JSON file."""
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.all_papers, f, indent=4)
        print(f"✅ Data saved to {filename} (Total papers: {len(self.all_papers)})")

    def run(self, output_file="ai_research_papers.json"):
        """Runs the full pipeline: fetching and saving papers."""
        self.collect_papers()
        # self.save_to_json(filename=output_file)
