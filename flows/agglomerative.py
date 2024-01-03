from metaflow import FlowSpec, Parameter, step


class AgglomerativeFlow(FlowSpec):
    table = Parameter(
        "table",
        default="vsi_tipitaka_preprocessed",
        help="Clean table to get training data from",
    )

    @step
    def start(self):
        from src import DuckbClient

        client = DuckbClient()
        self.df = client.execute_sql_string(f"SELECT * FROM {self.table}").pl()

        self.next(self.exclude)

    @step
    def exclude(self):
        from polars import exclude

        self.data = self.df.select(exclude(["volume_name", "basket"])).to_numpy()

        self.next(self.transform, self.reduce_dimensions)

    @step
    def transform(self):
        from sklearn.decomposition import PCA
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler

        transformer_pipeline = Pipeline(
            [
                ("scaler", StandardScaler()),
                ("pca", PCA(n_components=0.85)),  # PCA retaining 85% of variance
                # ("tsne", TSNE(3)),
            ]
        )

        self.X = transformer_pipeline.fit_transform(self.data)
        self.next(self.train)

    @step
    def train(self):
        from scipy.cluster.hierarchy import linkage
        from scipy.spatial.distance import pdist

        distance_matrix = pdist(self.X, metric="euclidean")

        self.linkage_matrix = linkage(distance_matrix, method="complete")

        self.next(self.assign_labels)

    @step
    def reduce_dimensions(self):
        from sklearn.manifold import TSNE
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler

        two_components_pipeline = Pipeline(
            [("scaler", StandardScaler()), ("tsne", TSNE())]
        )

        self.two_components = two_components_pipeline.fit_transform(self.data)

        self.next(self.assign_labels)

    @step
    def assign_labels(self, inputs):
        from pandas import DataFrame, concat

        self.merge_artifacts(inputs, include=["df", "linkage_matrix"])

        self.linkage_df = concat(
            [
                DataFrame(
                    self.linkage_matrix,
                    columns=["cluster_1", "cluster_2", "distance", "num_items"],
                ),
                DataFrame(
                    self.df.select("volume_name").to_numpy(), columns=["volume_name"]
                ),
            ],
            axis=1,
        )
        print(self.linkage_df[self.linkage_df.isna().any(axis=1)])
        self.linkage_df["cluster_1"] = self.linkage_df["cluster_1"].astype(int)
        self.linkage_df["cluster_2"] = self.linkage_df["cluster_2"].astype(int)

        self.next(self.end)

    @step
    def end(self):
        import matplotlib.pyplot as plt
        from scipy.cluster.hierarchy import dendrogram

        plt.figure(figsize=(10, 7))
        dn = dendrogram(
            self.linkage_matrix,
            orientation="top",
            labels=self.linkage_df["volume_name"].values,
            leaf_rotation=90.0,
        )
        dn

        plt.title("Hierarchical Clustering Dendrogram")
        plt.xlabel("Index of Text (or cluster)")
        plt.ylabel("Distance")

        plt.show()


if __name__ == "__main__":
    AgglomerativeFlow()
