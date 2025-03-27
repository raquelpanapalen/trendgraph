import networkx as nx

from src.preprocess import PaperRetriever


def build_knowledge_graph(openalex_data, semantic_scholar_data, arxiv_data):
    G = nx.DiGraph()

    # Add papers as nodes
    for paper in openalex_data + semantic_scholar_data + arxiv_data:
        paper_id = paper.get("title", "Unknown")
        G.add_node(paper_id, type="paper", data=paper)

    # Add citation relationships
    for paper in semantic_scholar_data:
        if "references" in paper:
            for ref in paper["references"]:
                ref_title = ref.get("title", "Unknown")
                G.add_edge(ref_title, paper["title"], relation="cites")

    return G


if __name__ == "__main__":
    paper_retriever = PaperRetriever()
    paper_retriever.run(output_file="data/openalex_research_papers.json")
