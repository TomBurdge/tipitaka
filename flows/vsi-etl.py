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

        self.file_name = url.split("/")[-1]

        self.next(self.write_names)

    @step
    def write_names(self):
        import os

        self.destination_path = os.path.join("data", "vri", "raw", "tipitaka_names.rda")

        with open(self.destination_path, "wb") as file:
            file.write(self.content)
        self.next(self.read_names)

    @step
    def read__raw_names(self):
        import pyreadr

        self.data = pyreadr.read_r(self.destination_path)
        self.next(self.names_to_df)

    @step
    def names_to_dataframe(self):
        import polars as pl

        self.df_names = pl.from_pandas(self.data["tipitaka_names"])

        self.next(self.modify_schema)

    @step
    def download_vri_tipitaka(self):
        from src import get_http

        url = f"https://raw.githubusercontent.com/dangerzig/tipitaka/master/data/{self.github_file}.rda"

        self.content = get_http(url)
        self.file_name = url.split("/")[-1]

        self.next(self.write_response)

    @step
    def write_response(self):
        import os

        self.destination_path = os.path.join("data", "vri", "raw", self.file_name)

        with open(self.destination_path, "wb") as file:
            file.write(self.content)

        self.next(self.read_raw)

    @step
    def read_raw(self):
        import pyreadr

        self.data = pyreadr.read_r(self.destination_path)

        self.next(self.to_dataframe)

    @step
    def to_dataframe(self):
        import polars as pl

        self.df = pl.from_pandas(self.data[self.github_file])

        self.next(self.modify_schema)

    @step
    def modify_schema(self):
        print(self.df.columns)
        print(self.df.head())
        self.df = self.df.select(["book", "word", "freq"]).rename(
            columns={"book": "volume_name"}
        )
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
