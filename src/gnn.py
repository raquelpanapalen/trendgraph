import torch
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import GCNConv
from neo4j import GraphDatabase
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error
import numpy as np
import random

# Connect to Neo4j
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "trendgraph")
driver = GraphDatabase.driver(URI, auth=AUTH)

# ----------- Neo4j Data Extraction -----------


def fetch_nodes_and_features():
    with driver.session() as session:
        result = session.run(
            """
            MATCH (p:Paper)
            WHERE p.embedding IS NOT NULL and p.citations IS NOT NULL and p.citations > 0 AND p.year >= 2008 AND p.year <= 2022
            RETURN p.paper_id as id, p.embedding AS features, p.year AS year, p.citations AS citations
        """
        )
        nodes = []
        for row in result:
            nodes.append(
                {
                    "id": row["id"],
                    "features": row["features"],
                    "citations": row["citations"],
                    "year": row["year"],
                }
            )
        return nodes


def fetch_edges():
    with driver.session() as session:
        result = session.run(
            """
            MATCH (p1:Paper)-[:CITES|RELATED|SIMILAR_TO]->(p2:Paper)
            RETURN p1.paper_id AS source, p2.paper_id AS target
        """
        )
        return [(r["source"], r["target"]) for r in result]


def fetch_title_and_abstract(paper_id):
    with driver.session() as session:
        result = session.run(
            """
            MATCH (p:Paper {paper_id: $paper_id})
            RETURN p.title AS title, p.abstract AS abstract
        """,
            paper_id=paper_id,
        )
        for row in result:
            return row["title"], row["abstract"]


# ----------- GNN Model -----------


class GCN(torch.nn.Module):
    def __init__(self, in_channels):
        super().__init__()
        self.conv1 = GCNConv(in_channels, 64)
        self.conv2 = GCNConv(64, 1)  # Output = regression

    def forward(self, x, edge_index):
        x = self.conv1(x, edge_index)
        x = F.relu(x)
        x = self.conv2(x, edge_index)
        return x.view(-1)


# ----------- Training Loop -----------


def train_model(data, train_mask, test_mask, y_true):
    model = GCN(data.num_node_features)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.008)
    loss_fn = torch.nn.MSELoss()

    for epoch in range(1000):
        model.train()
        optimizer.zero_grad()
        out = model(data.x, data.edge_index)
        loss = loss_fn(out[train_mask], y_true[train_mask])
        loss.backward()
        optimizer.step()

        if epoch % 20 == 0:
            model.eval()
            with torch.no_grad():
                pred = model(data.x, data.edge_index)
                rmse = root_mean_squared_error(
                    y_true[test_mask].cpu().numpy(),
                    pred[test_mask].cpu().numpy(),
                )
                print(f"Epoch {epoch}, RMSE: {rmse:.4f}")

    return model


# ----------- Main -----------


def main():
    print("Fetching data from Neo4j...")
    node_data = fetch_nodes_and_features()
    edge_list = set(fetch_edges())

    print(f"{len(node_data)} nodes and {len(edge_list)} edges fetched.")

    # Map node ids to indices
    id_map = {node["id"]: i for i, node in enumerate(node_data)}
    num_nodes = len(node_data)

    # Concatenate features with year
    x = torch.tensor(
        [np.concatenate([node["features"], [node["year"]]]) for node in node_data],
        dtype=torch.float,
    )
    # Normalize features
    x = (x - x.mean(dim=0)) / x.std(dim=0)

    # Target variable (citations), log-transformed to reduce skewness
    y = torch.log1p(
        torch.tensor([node["citations"] for node in node_data], dtype=torch.float)
    )

    # Edge index (2, num_edges)
    edges = [
        (id_map[src], id_map[tgt])
        for src, tgt in edge_list
        if src in id_map and tgt in id_map
    ]
    edge_index = torch.tensor(edges, dtype=torch.long).t().contiguous()

    # Train/test split
    train_idx, test_idx = train_test_split(
        range(num_nodes), test_size=0.2, random_state=42
    )
    train_mask = torch.zeros(num_nodes, dtype=torch.bool)
    test_mask = torch.zeros(num_nodes, dtype=torch.bool)
    train_mask[train_idx] = True
    test_mask[test_idx] = True

    # PyG Data object
    data = Data(x=x, edge_index=edge_index)

    print("Training GNN...")
    model = train_model(data, train_mask, test_mask, y)

    print("Finished training.")

    # Save the model
    print("Saving model...")
    torch.save(model.state_dict(), "models/gnn_model.pth")

    # After training
    model.eval()
    with torch.no_grad():
        predictions = model(data.x, data.edge_index)

    # Pick three examples from train and three from test
    example_indices = {
        "Train": [train_idx[random.randint(0, len(train_idx) - 1)] for _ in range(3)],
        "Test": [test_idx[random.randint(0, len(test_idx) - 1)] for _ in range(3)],
    }

    with open("output/example_predictions.txt", "w", encoding="utf-8") as f:
        for set_name, list_idx in example_indices.items():

            f.write(f"{set_name} Examples\n")
            f.write("=" * 60 + "\n")

            for idx in list_idx:
                paper_id = node_data[idx]["id"]
                pred_citations = torch.expm1(predictions[idx]).item()
                real_citations = node_data[idx]["citations"]
                title, abstract = fetch_title_and_abstract(paper_id)

                f.write(f"Title: {title}\n")
                f.write(f"Abstract: {abstract}\n")
                f.write(f"Year: {node_data[idx]['year']}\n")
                f.write(f"Predicted Citations: {pred_citations:.2f}\n")
                f.write(f"Real Citations: {real_citations}\n")
                f.write("\n" + "*" * 60 + "\n\n")

            f.write("=" * 60 + "\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    main()
