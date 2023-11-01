from metaflow import FlowSpec, step


class ETLFlow(FlowSpec):
    @step
    def start(self):
        import os

        from src import unzip_file

        self.pre_raw = os.path.join("data", "pre-raw", "2_pali.zip")
        self.raw = os.path.join("data", "raw")
        self.curated = os.path.join("data", "curated")
        self.staging = os.path.join("data", "staging")

        # unzip files
        unzip_file(self.pre_raw, self.raw)

        self.next(self.make_stop_words)

    @step
    def make_stop_words(self):
        from nltk.corpus import stopwords

        stop_words = stopwords.words("english") + [
            "th",
            "editions",
            "jaxxi",
            "jaxxii",
            "text",
            "society",
            "\x0e",
            "at",
            "edition",
            "pali",
            "printed",
            "see",
            "gretil",
            "sub",
            "uni",
            "goettingen",
            "file",
        ]

        self.stop_words = [f" {word} " for word in stop_words] + [
            "text",
            "society",
            "‘",
            "\x0e",
        ]

        self.next(self.list_raw)

    @step
    def list_raw(self):
        import os

        from src import list_files_recursive

        self.raw_files = list_files_recursive(self.raw)
        self.raw_files = [
            file
            for file in self.raw_files
            # make this ending into a param
            if os.path.splitext(os.path.split(file)[1])[0].endswith("pu")
        ]
        self.next(self.ingest, foreach="raw_files")

    @step
    def ingest(self):
        import os

        from src import html_to_text, remove_patterns, write_text

        self.raw_file = self.input

        file_name = self.raw_file.lstrip(str(self.raw)).rstrip(".htm")
        file_output = os.path.join(self.curated, file_name) + ".txt"
        text = html_to_text(self.raw_file)
        text = remove_patterns(text, self.stop_words)
        write_text(text, file_output)

        self.next(self.join)

    @step
    def join(self, inputs):
        self.merge_artifacts(inputs, exclude="raw_file")
        self.next(self.list_curated)

    @step
    def list_curated(self):
        import os

        from src import list_files_recursive

        curated_files = list_files_recursive(self.curated)
        self.curated_files = [
            file
            for file in curated_files
            if os.path.splitext(os.path.split(file)[1])[0].endswith("pu")
        ]

        self.next(self.read_curated, foreach="curated_files")

    @step
    def read_curated(self):
        from src import txt_to_text

        self.curated_file = self.input

        text = txt_to_text(self.curated_file)
        # risky new code - getting the English stuff at the start of pu
        self.text = text.split("further information ")[-1]
        self.next(self.tokenize)

    @step
    def tokenize(self):
        from src import tokenize_text

        self.tokens = tokenize_text(self.text)
        self.next(self.count)

    @step
    def count(self):
        from collections import Counter

        from src import relative_frequency

        word_counts = Counter(self.tokens)
        total_words = sum(word_counts.values())
        frequencies = relative_frequency(word_counts, total_words)
        self.words = list(frequencies.keys())
        self.frequencies = list(frequencies.values())

        self.next(self.to_clean_parquet)

    @step
    def to_clean_parquet(self):
        import os

        from polars import LazyFrame, lit

        vol_name = os.path.splitext(os.path.split(self.curated_file)[-1])[0]
        lf = LazyFrame(
            {"words": self.words, "frequencies": self.frequencies}
        ).with_columns(lit(vol_name).alias("volume_name"))
        self.output_filename = (
            os.path.join(
                self.staging, self.curated_file.lstrip(self.curated).rstrip(".txt")
            )
            + ".parquet"
        )
        os.makedirs(os.path.dirname(self.output_filename), exist_ok=True)
        lf.sink_parquet(self.output_filename)

        self.next(self.join2)

    @step
    def join2(self, inputs):
        self.clean_file_names = [input.output_filename for input in inputs]
        self.next(self.clean)

    @step
    def clean(self):
        import os

        from src import DuckbClient, list_files_recursive

        # (unfortunately) DuckDB doesn't support multiple concurrent connections yet
        # (there's somme interesting stuff going on in the buena vista project, which simulates postgresql
        # but buena vista is fairly early stage)
        # https://github.com/jwills/buenavista
        # even if DuckDB did support this, I wouldn't want lots of concurrent connections and writes
        # metaflow does not currently support branch specific concurrency (see their github issue #172)
        # so, unfortunately there is a non-parallel for-loop
        # fortunately, this is unlikely to be a bottleneck because parquet to duckdb is zero-copy

        self.staging = os.path.join("data", "staging")

        tipitaka = os.path.join(self.staging, "2_pali", "1_tipit")
        baskets = ["vin", "sut", "abh"]

        # aim - eliminate the recursive file finding by passing along the clean file names
        # need to add a way to get the basket just from the string to do this
        for i, basket in enumerate(baskets, 1):
            basket_path = os.path.join(tipitaka, f"{str(i)}_{basket}")

            files = list_files_recursive(basket_path)

            # filter to only pts files
            pts_basket = []
            for file in files:
                raw_filename = os.path.splitext(os.path.split(file)[1])[0]
                if raw_filename.endswith("pu"):
                    pts_basket.append(file)
                else:
                    continue

            client = DuckbClient()
            client.parquets_to_table(basket, pts_basket)
        self.next(self.collect_baskets)

    @step
    def collect_baskets(self):
        from src import DuckbClient

        # you can't pickle this client so it can't be passed between jobs and kept a singleton
        client = DuckbClient()
        self.lf = (
            client.execute_sql_string("""SELECT *, 'vin' as basket FROM vin""")
            .pl()
            .vstack(
                client.execute_sql_string("""SELECT *, 'sut' as basket FROM sut""").pl()
            )
            .vstack(
                client.execute_sql_string("""SELECT *, 'abh' as basket FROM abh""").pl()
            )
        ).lazy()

        self.next(self.top_x_words)

    @step
    def top_x_words(self):
        from polars import col

        self.top_1000_words = (
            self.lf.group_by("words")
            .agg(col("frequencies").sum())
            .sort("frequencies", descending=True)
            .limit(1000)
        )

        self.next(self.filter_to_top_x)

    @step
    def filter_to_top_x(self):
        from polars import col

        self.df = (
            self.lf.filter(
                col("words").is_in(
                    self.top_1000_words.select(col("words")).collect().to_series()
                )
            )
            .collect()
            .pivot(
                index=["volume_name", "basket"], columns="words", values="frequencies"
            )
            .fill_null(0)
        )

        self.next(self.end)

    @step
    def end(self):
        from src import DuckbClient

        client = DuckbClient()

        client.execute_sql_string("DROP TABLE IF EXISTS tipitaka_preprocessed")

        self.result = client.df_to_table("tipitaka_preprocessed", self.df)


if __name__ == "__main__":
    ETLFlow()
