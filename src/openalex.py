import requests
import json


def invert_abstract_index(abstract_inverted_index):
    if not abstract_inverted_index:
        return None

    abstract_index = {}
    for k, vlist in abstract_inverted_index.items():
        for v in vlist:
            abstract_index[v] = k
    return " ".join(abstract_index[k] for k in sorted(abstract_index.keys()))


def format_paper(paper):
    return {
        "paper_id": paper.get("id", None),
        "title": paper.get("title", None),
        "doi": paper.get("doi", None),
        "year": paper.get("publication_year", None),
        "citations": paper.get("cited_by_count", None),
        "abstract": invert_abstract_index(paper.get("abstract_inverted_index", {})),
        "topics": [topic.get("display_name", {}) for topic in paper.get("topics", [])],
    }
