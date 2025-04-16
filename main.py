from src.preprocess import PaperRetriever


if __name__ == "__main__":
    paper_retriever = PaperRetriever()
    paper_retriever.run(output_file="data/openalex_research_papers.json")
