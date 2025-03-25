import requests
import json


def get_publisher(paper):
    if paper.get("primary_location", {}) is not None:
        source = paper.get("primary_location", {}).get("source", {})
        if source is not None:
            return source.get("display_name", None)

    return None


def invert_abstract_index(abstract_inverted_index):
    if not abstract_inverted_index:
        return None

    abstract_index = {}
    for k, vlist in abstract_inverted_index.items():
        for v in vlist:
            abstract_index[v] = k
    return " ".join(abstract_index[k] for k in sorted(abstract_index.keys()))


def format_paper(id, paper, topic, cited_by=[], references_to=[]):
    return {
        "paper_id": id,
        "title": paper.get("title", None),
        "doi": paper.get("doi", None),
        "authors": [
            {
                "author_name": author_info.get("author", {}).get("display_name", None),
                "institutions": [
                    institution.get("display_name", None)
                    for institution in author_info.get("institutions", [])
                ],
            }
            for author_info in paper.get("authorships", [])
        ],
        "year": paper.get("publication_year", None),
        "citations": paper.get("cited_by_count", None),
        "abstract": invert_abstract_index(paper.get("abstract_inverted_index", {})),
        "publisher": get_publisher(paper),
        "topic": topic,
        "cited_by": cited_by,
        "references_to": references_to,
    }
