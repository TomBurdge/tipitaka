from metaflow import FlowSpec, step


class ETLFlow(FlowSpec):
    @step
    def start(self):
        import os

        from src import unzip_file

        self.pre_raw = os.path.join("data", "pre-raw", "2_pali.zip")
        self.raw = os.path.join("data", "raw")
        self.curated = os.path.join("data", "curated")

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
        from src import list_files_recursive

        self.rawfiles = list_files_recursive(self.raw)
        self.next(self.ingest, foreach="rawfiles")

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
        self.next(self.end)

    @step
    def end(self):
        print("finished")


if __name__ == "__main__":
    ETLFlow()
