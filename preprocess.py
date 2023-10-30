import polars as pl

from src import DuckbClient

if __name__ == "__main__":
    client = DuckbClient()
    baskets = ["vin", "sut", "abh"]
    lf = (
        client.execute_sql_string("""SELECT *, 'vin' as basket FROM vin""")
        .pl()
        .vstack(
            client.execute_sql_string("""SELECT *, 'sut' as basket FROM sut""").pl()
        )
        .vstack(
            client.execute_sql_string("""SELECT *, 'abh' as basket FROM abh""").pl()
        )
    ).lazy()
    # The 1,000 words with the highest average (relative) frequency across all volumes
    # of the Canon are selected as features.
    top_1000_words = (
        lf.group_by(["words", "basket"])
        .agg(pl.col("frequencies").sum())
        .sort("frequencies", descending=True)
        .limit(1000)
    )
    df = (
        lf.filter(
            pl.col("words").is_in(
                top_1000_words.select(pl.col("words")).collect().to_series()
            )
        )
        .collect()
        .pivot(index=["volume_name", "basket"], columns="words", values="frequencies")
        .fill_null(0)
    )
    client.execute_sql_string("DROP TABLE IF EXISTS tipitaka_preprocessed")
    result = client.df_to_table("tipitaka_preprocessed", df)
