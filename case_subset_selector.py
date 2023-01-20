import dask.dataframe as dd
import pandas as pd

from dask.distributed import Client

case_type = "cc"
act = 17353
section = 865403

if __name__ == "__main__":
    client = Client()
    print(client)

    date_parser = lambda x: pd.to_datetime(str(x), errors='coerce', format="%d-%m-%Y")
    case_date_parser = lambda x: pd.to_datetime(str(x), errors='coerce', format="%Y-%m-%d")

    cases_dtype = {
        "ddl_case_id": "string[pyarrow]",
        "year": "int16",
        "state_code": "category",
        "dist_code": "category",
        "court_no": "category",
        "cino": "string[pyarrow]",
        'judge_position': "string[pyarrow]",
        "female_defendant": "category",
        "female_adv_def": "category",
        "female_adv_pet": "category",
        "type_name": "Int16",
        "purpose_name": "Int16",
        "disp_name": "Int16",
        "date_of_filing": "string[pyarrow]",
        "date_of_decision": "string[pyarrow]",
        "date_first_list": "string[pyarrow]",
        "date_last_list": "string[pyarrow]",
        "date_next_list": "string[pyarrow]",
        "act": "Int16",
        "section": "Int64",
        "bailable_ipc": "category",
        "number_sections_ipc": "Int16",
        "criminal": "category"
    }

    cases = dd.concat([dd.read_parquet(f"./Parquet/cases_acts/{year}/", dtype=cases_dtype,
                                       parse_dates=["date_of_filing", "date_of_decision"]) for year in
                       range(2010, 2019)])

    type_name_key = dd.read_csv("./csv/keys/keys/type_name_key.csv",
                                dtype={"year": "int16", "type_name": "Int16", "type_name_s": "string[pyarrow]",
                                       "count": "int64"})

    # %%
    cases = dd.merge(cases, type_name_key[["year", "type_name", "type_name_s"]], how="left",
                     on=["year", "type_name"])
    # %%
    disp_name_key = dd.read_csv("./csv/keys/keys/disp_name_key.csv",
                                dtype={"year": "int16", "disp_name": "Int16", "disp_name_s": "string[pyarrow]",
                                       "count": "int64"})

    # %%
    cases = dd.merge(cases, disp_name_key[["year", "disp_name", "disp_name_s"]], how="left",
                     on=["year", "disp_name"])
    # cases.head()
    # %%
    cases = cases[(cases["type_name_s"] == case_type) & (cases["act"] == act) & (cases["section"] == section)]
    # cases
    # %%
    finished_cases = cases.dropna(subset=["date_of_filing", "date_of_decision", "disp_name"])
    finished_cases = finished_cases[
        ((finished_cases["date_of_decision"] - finished_cases["date_of_filing"]).dt.total_seconds() >= 0) &
        ((pd.to_datetime('today') - finished_cases["date_of_decision"]).dt.total_seconds() >= 0) &
        (finished_cases["date_of_filing"].dt.year == finished_cases["year"])
        ]
    finished_cases["duration"] = (finished_cases["date_of_decision"] - finished_cases["date_of_filing"]).dt.days
    # %%
    f_cases_id = dd.concat([dd.read_parquet(f"./Parquet/cases/{year}/", dtype=cases_dtype,
                                            columns=["ddl_case_id", "cino"],
                                            parse_dates=["date_of_filing", "date_of_decision"]) for year in
                            range(2010, 2019)])
    finished_cases = dd.merge(finished_cases.compute(), f_cases_id, how="inner", on="cino")

    features_classes = finished_cases[
        ["ddl_case_id", "duration", "year", "state_code", "dist_code", "court_no", "female_defendant",
         "female_petitioner", "female_adv_def", "female_adv_pet", "disp_name_s"]]

    features_classes["state_code"] = features_classes["state_code"].astype("int64")
    features_classes["dist_code"] = features_classes["dist_code"].astype("int64")
    features_classes["court_no"] = features_classes["court_no"].astype("int64")

    features_classes["female_defendant"] = features_classes["female_defendant"].astype("category").cat.as_known()
    features_classes["female_petitioner"] = features_classes["female_petitioner"].astype("category").cat.as_known()
    features_classes["female_adv_def"] = features_classes["female_adv_def"].astype("category").cat.as_known()
    features_classes["female_adv_pet"] = features_classes["female_adv_pet"].astype("category").cat.as_known()
    features_classes["disp_name_s"] = features_classes["disp_name_s"].astype("category").cat.as_known()
    features_classes["fd"] = features_classes["female_defendant"].cat.codes
    features_classes["fp"] = features_classes["female_petitioner"].cat.codes
    features_classes["fad"] = features_classes["female_adv_def"].cat.codes
    features_classes["fap"] = features_classes["female_adv_pet"].cat.codes

    features_classes["state_code"] = features_classes["state_code"].astype("int64")
    features_classes["dist_code"] = features_classes["dist_code"].astype("int64")
    features_classes["court_no"] = features_classes["court_no"].astype("int64")

    features_classes["dp"] = features_classes["disp_name_s"].cat.codes

    # %%
    features_classes.to_parquet(f"Parquet/features_classes2/", engine="fastparquet")
