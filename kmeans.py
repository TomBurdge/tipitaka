import os

import altair as alt
import polars as pl
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src import DuckbClient

if __name__ == "__main__":
    client = DuckbClient()
    df = client.execute_sql_string("SELECT * FROM tipitaka_preprocessed").pl()

    kmeans_pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("pca", PCA(n_components=0.85)),  # PCA retaining 95% of variance
            ("dbscan", KMeans(n_clusters=2)),
        ]
    )
    data = df.select(pl.exclude(["volume_name", "basket"])).to_numpy()

    kmeans_pipeline.fit(data)

    labels = kmeans_pipeline.predict(data)

    df = df.with_columns(pl.Series(labels).alias("labels"))

    two_components_pipeline = Pipeline(
        [("scaler", StandardScaler()), ("pca", PCA(n_components=2))]
    )
    two_components = two_components_pipeline.fit_transform(data)
    df = df.with_columns(
        pl.DataFrame(
            two_components,
            schema={"component_1": pl.Float64, "component_2": pl.Float64},
        )
    )

    chart = (
        alt.Chart(
            df.select(
                ["component_2", "component_1", "labels", "volume_name", "basket"]
            ).to_pandas()
        )
        .mark_circle(size=60)
        .encode(
            x="component_2",
            y="component_1",
            color="labels",
            tooltip=["volume_name", "labels", "basket"],
        )
        .interactive()
    )

    chart.save(os.path.join("visualisations", "kmeans", "chart.html"))
