import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import os
from pathlib import Path

output_dir = Path(os.path.dirname(__file__)).parent / "output"

# Load the CSV file
df = pd.read_csv(f"{output_dir}/emerging_topics.csv")

# Ensure correct data types
df["year"] = df["year"].astype(int)
df["papers_published"] = df["papers_published"].astype(int)

# Only plot since 2010 until 2022 (2023-2025 incomplete data)
df = df[df["year"].between(2010, 2022)]

# Get the top 10 topics per year
top_topics_per_year = (
    df.sort_values(["year", "papers_published"], ascending=[True, False])
    .groupby("year")
    .head(10)
)

# Get unique top topics across all years
top_topics_overall = top_topics_per_year["topic"].unique()

# Filter the main dataframe to include only these topics
df_top = df[df["topic"].isin(top_topics_overall)]

# Pivot to get a dataframe with years as index and topics as columns
pivot_df = df_top.pivot_table(
    index="year", columns="topic", values="papers_published", fill_value=0
)

# Get increase between 2010 and 2022
increase = pivot_df.loc[2022] - pivot_df.loc[2010]
increase = increase.sort_values(ascending=False)

# Top 5 and bottom 5 topics by increase
top5_increase = increase.head(5).index
bottom5_increase = increase.tail(5).index

# Get ranked topic names
ranked_topics = increase.index.tolist()
rank_dict = {topic: rank + 1 for rank, topic in enumerate(ranked_topics)}

# Colors for highlighted topics
palette = sns.color_palette("tab10", 10)
highlight_colors = {
    topic: palette[i] for i, topic in enumerate(top5_increase.union(bottom5_increase))
}

# Plotting
fig, ax = plt.subplots(figsize=(12, 8))
sns.set_theme(style="whitegrid")

for topic in pivot_df.columns:
    rank = rank_dict.get(topic, "-")
    if topic in top5_increase or topic in bottom5_increase:
        ax.plot(
            pivot_df.index,
            pivot_df[topic],
            label=f"{rank}. {topic}",
            linewidth=2.5,
            color=highlight_colors[topic],
        )
    else:
        ax.plot(
            pivot_df.index,
            pivot_df[topic],
            label=f"{rank}. {topic}",
            linestyle="--",  # Dashed line for non-highlighted topics
            color="lightgrey",
            linewidth=1,
            alpha=0.7,
        )


handles, labels = ax.get_legend_handles_labels()
sorted_handles_labels = sorted(
    zip(handles, labels), key=lambda x: int(x[1].split(".")[0])
)

handles, labels = zip(*sorted_handles_labels)

ax.set_title("Top 10 Topics per Year — Growth Highlighted (2010–2022)", fontsize=16)
ax.set_xlabel("Year")
ax.set_ylabel("Number of Papers Published")
ax.legend(handles, labels, title="Topic", bbox_to_anchor=(1.05, 1), loc="upper left")
plt.tight_layout()
plt.savefig(f"{output_dir}/emerging_topics_growth.png", dpi=300)
