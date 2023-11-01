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
        self.next(self.end)

    @step
    def end(self):
        print("finished")


if __name__ == "__main__":
    ETLFlow()
