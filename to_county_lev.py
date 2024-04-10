import pandas as pd

# National Climatic Data Center (NCDC) state codes are different than FIPS!

crosswalk = pd.read_csv("counties_crosswalk.csv", dtype=str)
crosswalk["state_code"] = crosswalk["POSTAL_FIPS_ID"].apply(
    lambda item: f"{item:>05}"[:2]
)
crosswalk["county_code"] = crosswalk["POSTAL_FIPS_ID"].apply(
    lambda item: f"{item:>05}"[2:]
)
crosswalk["state_ncdc"] = crosswalk["NCDC_FIPS_ID"].apply(
    lambda item: f"{item:>05}"[:2]
)

# NCDC county is the same as census county
crosswalk["county_nat_cli_data_cen"] = crosswalk["POSTAL_FIPS_ID"].apply(
    lambda item: f"{item:>05}"[2:]
)

cbsa_crosswalk = pd.read_csv("cbsa_crosswalk.csv", dtype=str)

merged_to_ncdc = cbsa_crosswalk.merge(
    crosswalk, on=["state_code", "county_code"], how="left"
)

ave_temps = pd.read_csv("ave_temps.csv", dtype=str)

merged_to_temps = ave_temps.rename(
    columns={"state_code": "state_ncdc", "division_number": "county_code"}
).merge(
    merged_to_ncdc,
    on=["state_ncdc", "county_code"],
)

precip = pd.read_csv("precip.csv", dtype=str)

merged_to_precip = precip.rename(
    columns={"state_code": "state_ncdc", "division_number": "county_code"}
).merge(
    merged_to_ncdc,
    on=["state_ncdc", "county_code"],
)

study_years_1980 = {"1976", "1977", "1978", "1979", "1980"}

precip_1980 = merged_to_precip[merged_to_precip["year"].isin(study_years_1980)]

precip_1980.astype(
    {
        "jan_value": "float",
        "feb_value": "float",
        "mar_value": "float",
        "apr_value": "float",
        "may_value": "float",
        "june_value": "float",
        "july_value": "float",
        "aug_value": "float",
        "sept_value": "float",
        "oct_value": "float",
        "nov_value": "float",
        "dec_value": "float",
    }
).groupby("cbsa_code").agg(
    {
        "cbsa_title": "first",
        "jan_value": "mean",
        "feb_value": "mean",
        "mar_value": "mean",
        "apr_value": "mean",
        "may_value": "mean",
        "june_value": "mean",
        "july_value": "mean",
        "aug_value": "mean",
        "sept_value": "mean",
        "oct_value": "mean",
        "nov_value": "mean",
        "dec_value": "mean",
    }
).reset_index().to_csv(
    "cbsa_1980_precip.csv", index=False
)


temps_1980 = merged_to_temps[merged_to_temps["year"].isin(study_years_1980)]

temps_1980.astype(
    {
        "jan_value": "float",
        "feb_value": "float",
        "mar_value": "float",
        "apr_value": "float",
        "may_value": "float",
        "june_value": "float",
        "july_value": "float",
        "aug_value": "float",
        "sept_value": "float",
        "oct_value": "float",
        "nov_value": "float",
        "dec_value": "float",
    }
).groupby("cbsa_code").agg(
    {
        "cbsa_title": "first",
        "jan_value": "mean",
        "feb_value": "mean",
        "mar_value": "mean",
        "apr_value": "mean",
        "may_value": "mean",
        "june_value": "mean",
        "july_value": "mean",
        "aug_value": "mean",
        "sept_value": "mean",
        "oct_value": "mean",
        "nov_value": "mean",
        "dec_value": "mean",
    }
).reset_index().to_csv(
    "cbsa_1980_temps.csv", index=False
)

study_years_2020 = {"2020", "2019", "2018", "2017", "2016"}

precip_2020 = merged_to_precip[merged_to_precip["year"].isin(study_years_2020)]

precip_2020.astype(
    {
        "jan_value": "float",
        "feb_value": "float",
        "mar_value": "float",
        "apr_value": "float",
        "may_value": "float",
        "june_value": "float",
        "july_value": "float",
        "aug_value": "float",
        "sept_value": "float",
        "oct_value": "float",
        "nov_value": "float",
        "dec_value": "float",
    }
).groupby("cbsa_code").agg(
    {
        "cbsa_title": "first",
        "jan_value": "mean",
        "feb_value": "mean",
        "mar_value": "mean",
        "apr_value": "mean",
        "may_value": "mean",
        "june_value": "mean",
        "july_value": "mean",
        "aug_value": "mean",
        "sept_value": "mean",
        "oct_value": "mean",
        "nov_value": "mean",
        "dec_value": "mean",
    }
).reset_index().to_csv(
    "cbsa_2020_precip.csv", index=False
)


temps_2020 = merged_to_temps[merged_to_temps["year"].isin(study_years_2020)]

temps_2020.astype(
    {
        "jan_value": "float",
        "feb_value": "float",
        "mar_value": "float",
        "apr_value": "float",
        "may_value": "float",
        "june_value": "float",
        "july_value": "float",
        "aug_value": "float",
        "sept_value": "float",
        "oct_value": "float",
        "nov_value": "float",
        "dec_value": "float",
    }
).groupby("cbsa_code").agg(
    {
        "cbsa_title": "first",
        "jan_value": "mean",
        "feb_value": "mean",
        "mar_value": "mean",
        "apr_value": "mean",
        "may_value": "mean",
        "june_value": "mean",
        "july_value": "mean",
        "aug_value": "mean",
        "sept_value": "mean",
        "oct_value": "mean",
        "nov_value": "mean",
        "dec_value": "mean",
    }
).reset_index().to_csv(
    "cbsa_2020_temps.csv", index=False
)
