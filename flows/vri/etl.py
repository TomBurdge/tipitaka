from metaflow import FlowSpec, step


class ETLFlow(FlowSpec):
    @step
    def start(self):
        import os

        # move to top directory
        os.chdir(os.path.join(os.getcwd(), "..", ".."))

        self.next(self.download_vri_tipitaka)

    @step
    def download_vri_tipitaka(self):
        import requests

        url = "https://raw.githubusercontent.com/dangerzig/tipitaka/master/data/tipitaka_long.rda"
        response = requests.get(url, allow_redirects=True)

        response.raise_for_status()

        self.content = response.content
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

        df = pl.from_pandas(self.data["tipitaka_long"])
        print(df.select("book").unique().sort(by="book").to_series().to_list())
        self.next(self.end)

    @step
    def end(self):
        print("Finished!")


if __name__ == "__main__":
    ETLFlow()
