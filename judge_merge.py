import dask.dataframe as dd
from dask.distributed import Client

if __name__ == "__main__":
    client = Client()
    print(client)
    fc = dd.read_parquet("Parquet/features_classes/")
    fc = fc.compute()

    judge_data = dd.read_csv("csv/keys/keys/judge_case_merge_key.csv", dtype={
        "ddl_case_id": "string[pyarrow]",
        "ddl_filing_judge_id": "Int32",
        "ddl_decision_judge_id": "Int32"
    })
    fc = dd.merge(fc, judge_data, how="inner", on="ddl_case_id")
    print(fc.count().compute())
    fc = fc.dropna(subset=["ddl_decision_judge_id"])
    print(fc.count().compute())
    fc.to_parquet("Parquet/features_classes2/")
    print("wait")
    while True:
        pass
