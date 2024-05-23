import sys
from pathlib import Path

import polars as pl


def read_table(table_version: int | None):
    file_path = "delta/source_table_bronze"

    kwargs = dict(source=file_path)

    if table_version is not None:
        delta_log_files = (Path(file_path) / "_delta_log").glob("*")
        max_table_version = max(
            int(i.name.split(".")[0]) for i in delta_log_files if i.is_file()
        )
        if table_version > max_table_version:
            print(f"Version requested does not exist. Max version: {max_table_version}")
            sys.exit()
        kwargs["version"] = table_version

    df = pl.read_delta(**kwargs)
    print(df)


def create_table(feed_number: int = 1):
    """Create source data

    Create the source data for use in the delta lake example

    Args:
        feed_number (int): number to separate the different source files
    """
    file_name = f"source_data/feed_{feed_number}.parquet"
    data = {
        "id": [1, 2],
        "name": ["nice", "bad"],
        "amount": [100, 123],
    }

    id_increment = 2 * (feed_number - 1)
    data["id"] = [i + id_increment for i in data["id"]]
    data["name"] = [f"{i}-{feed_number}" for i in data["name"]]
    data["amount"] = [i * feed_number for i in data["amount"]]

    df = pl.DataFrame(data)

    print(df)
    df.write_parquet(file_name)


if __name__ == "__main__":
    args = sys.argv
    table_version = None
    if len(args) > 1:
        table_version = int(args[1])

    read_table(table_version)
    # create_table(feed_number=table_version)
