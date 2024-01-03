from metaflow import FlowSpec, Parameter, step


class ClusterTrainFlow(FlowSpec):
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

        # from sklearn.manifold import TSNE
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler

        transformer_pipeline = Pipeline(
            [
                ("scaler", StandardScaler()),
                ("pca", PCA(n_components=0.95)),  # PCA retaining 85% of variance
                # ("tsne", TSNE(3)),
            ]
        )

        self.X = transformer_pipeline.fit_transform(self.data)
        self.next(self.train)

    @step
    def train(self):
        from sklearn.cluster import KMeans

        model = KMeans(n_clusters=2)
        self.labels = model.fit_predict(self.X)
        self.next(self.assign_labels)

    @step
    def reduce_dimensions(self):
        from sklearn.decomposition import PCA
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler

        two_components_pipeline = Pipeline(
            [("scaler", StandardScaler()), ("tsne", PCA(n_components=2))]
        )

        self.two_components = two_components_pipeline.fit_transform(self.data)

        self.next(self.assign_labels)

    @step
    def assign_labels(self, inputs):
        from polars import DataFrame, Float64, Series

        self.merge_artifacts(inputs, include=["df"])

        result = self.df.with_columns(
            DataFrame(
                inputs.reduce_dimensions.two_components,
                schema={"component_1": Float64, "component_2": Float64},
            )
        ).with_columns(Series(inputs.train.labels).alias("labels"))

        self.plot_df = result.select(
            ["component_2", "component_1", "labels", "volume_name", "basket"]
        ).to_pandas()

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ClusterTrainFlow()
