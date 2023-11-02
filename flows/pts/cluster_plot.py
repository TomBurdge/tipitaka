import os

import altair as alt
from metaflow import Flow
from ulid import ULID

# move to top directory
os.chdir(os.path.join(os.getcwd(), "..", ".."))

run = Flow("ClusterTrainFlow").latest_run

plot_df = run["assign_labels"].task.data.plot_df

chart1 = (
    alt.Chart(plot_df)
    .mark_circle(size=60)
    .encode(
        x="component_2",
        y="component_1",
        color="labels",
        tooltip=["volume_name", "labels", "basket"],
    )
    .interactive()
)

chart2 = (
    alt.Chart(plot_df)
    .mark_circle(size=60)
    .encode(
        x="component_2",
        y="component_1",
        color="basket",
        tooltip=["volume_name", "labels", "basket"],
    )
    .interactive()
)

combined_chart = alt.vconcat(chart1, chart2)

combined_chart.save(
    os.path.join("visualisations", "kmeans", f"{str(ULID())}-chart.html")
)
