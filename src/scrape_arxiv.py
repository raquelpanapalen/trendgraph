import urllib
import feedparser


def fetch_arxiv_papers(query, num_results=10):
    base_url = "http://export.arxiv.org/api/query"

    if " " in query:
        query = "%22" + query.replace(" ", "+") + "%22"
    search_query = f"search_query=all:{query}&start=0&max_results={num_results}"

    # perform a GET request using the base_url and query
    with urllib.request.urlopen(f"{base_url}?{search_query}") as url:
        response = url.read()

    feed = feedparser.parse(response)

    papers = []
    for entry in feed.entries:
        papers.append(
            {
                "title": entry.title,
                "authors": [author.name for author in entry.authors],
                "summary": entry.summary,
                "published": entry.published,
                "arxiv:journal_ref": entry.get("arxiv_journal_ref", None),
            }
        )
    return papers
