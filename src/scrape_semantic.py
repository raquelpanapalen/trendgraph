import requests


def fetch_semantic_scholar_papers(query, num_results=10):
    base_url = "https://api.semanticscholar.org/graph/v1/paper/search"
    params = {
        "query": query,
        "fields": "title,authors,abstract,year,venue,citationCount,references,externalIds",
        "limit": num_results,
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json().get("data", [])
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return []
