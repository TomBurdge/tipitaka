import os

from tqdm import tqdm

from src import DuckbClient, list_files_recursive

# pts with no notes ends with pu
if __name__ == "__main__":
    clean_zone = os.path.join("data", "clean")

    staging_zone = os.path.join("data", "staging")

    tipitaka = os.path.join(staging_zone, "2_pali", "1_tipit")

    baskets = ["vin", "sut", "abh"]
    for i, basket in tqdm(enumerate(baskets, 1)):
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

        df = client.select_columns_from_table(
            basket, ["words", "frequencies", "volume_name"]
        ).pl()
