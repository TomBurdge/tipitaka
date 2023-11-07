from metaflow import FlowSpec, Parameter, step


class ETLFlow(FlowSpec):
    github_file = Parameter(
        "file_name", help="file to fetch from R Tipitaka", default="tipitaka_long"
    )

    @step
    def start(self):
        self.next(self.download_vri_tipitaka, self.download_names)

    @step
    def download_names(self):
        from src import get_http

        url = "https://raw.githubusercontent.com/dangerzig/tipitaka/master/data/tipitaka_names.rda"

        self.content = get_http(url)

        self.next(self.write_names)

    @step
    def write_names(self):
        import os

        self.destination_path = os.path.join("data", "vri", "raw", "tipitaka_names.rda")

        with open(self.destination_path, "wb") as file:
            file.write(self.content)
        self.next(self.read_names)

    @step
    def read_names(self):
        from pyreadr import read_r

        self.names = read_r(self.destination_path)
        self.next(self.names_to_dataframe)

    @step
    def names_to_dataframe(self):
        import polars as pl

        self.df_names = pl.from_pandas(self.names["tipitaka_names"])

        self.next(self.decode_names_df)

    @step
    def decode_names_df(self):
        from polars import col
        from src import decode_string

        self.df_names = self.df_names.with_columns(
            col("name").map_elements(lambda x: decode_string(x))
        ).with_columns(
            col("name").str.split(" ").list.first().alias("basket"),
            col("name")
            .str.split(" ")
            .list.reverse()
            .list.head(2)
            .list.join(" ")
            .alias("volume_name"),
        )

        self.next(self.modify_schema)

    @step
    def download_vri_tipitaka(self):
        from src import get_http

        url = f"https://raw.githubusercontent.com/dangerzig/tipitaka/master/data/{self.github_file}.rda"

        self.raw_content = get_http(url)
        self.raw_file_name = url.split("/")[-1]

        self.next(self.write_response)

    @step
    def write_response(self):
        import os

        self.raw_file_path = os.path.join("data", "vri", "raw", self.raw_file_name)

        with open(self.raw_file_path, "wb") as file:
            file.write(self.raw_content)

        self.next(self.read_raw)

    @step
    def read_raw(self):
        from pyreadr import read_r

        self.data = read_r(self.raw_file_path)

        self.next(self.to_dataframe)

    @step
    def to_dataframe(self):
        import polars as pl

        self.lf = pl.from_pandas(self.data[self.github_file]).lazy()

        self.next(self.find_top_1000_words)

    @step
    def find_top_1000_words(self):
        from polars import col

        self.top_1000_words = (
            self.lf.group_by("word")
            .agg(col("freq").sum())
            .sort("freq", descending=True)
            .limit(1000)
        )
        self.df = (
            self.lf.filter(
                col("word").is_in(
                    self.top_1000_words.select(col("word")).collect().to_series()
                )
            )
            .collect()
            .pivot(index=["book"], columns="word", values="freq")
            .fill_null(0)
        )
        self.next(self.modify_schema)

    @step
    def modify_schema(self, inputs):
        self.merge_artifacts(inputs, include=["df", "df_names"])
        self.df = self.df.join(self.df_names, on="book")

        self.next(self.to_duckdb)

    @step
    def to_duckdb(self):
        from src import DuckbClient

        client = DuckbClient()
        client.execute_sql_string("DROP TABLE IF EXISTS vsi_tipitaka_preprocessed")

        self.result = client.df_to_table("vsi_tipitaka_preprocessed", self.df)
        self.next(self.end)

    @step
    def end(self):
        print("Finished!")


if __name__ == "__main__":
    ETLFlow()
