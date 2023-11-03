from metaflow import FlowSpec, Parameter, step


class KMeansTrainFlow(FlowSpec):
    table = Parameter(
        "table",
        default="tipitaka_preprocessed",
        help="Clean table to get training data from",
    )

    @step
    def start(self):
        from src import DuckbClient

        client = DuckbClient()
        self.df = client.execute_sql_string(f"SELECT * FROM {self.table}").pl()

        self.next(self.end)

    @step
    def end(self):
        print(self.df)


if __name__ == "__main__":
    KMeansTrainFlow()
