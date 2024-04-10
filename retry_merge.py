import pandas as pd
import numpy as np

from d3census import censusify, Geography, Edition
from d3census.profile import build_profile


# Network the cities in a d3 simulation

POP_WEIGHT = 0.001
TEMP_WEIGHT = 0.001
PRECIP_WEIGHT = 0.001
INDUSTRY_WEIGHT = 1
INCOME_WEIGHT = 0.001


def pad_geoid(geoid):
    return f"{geoid:0>5}"

try:
    demos = pd.read_csv("demos.csv")

except FileNotFoundError as e:

    @censusify
    def total_population(geo: Geography):
        return geo.B01001._001E

    @censusify
    def agriculture(geo: Geography):
        return geo.B24032._002E + geo.B24032._029E

    @censusify
    def construction(geo: Geography):
        return geo.B24032._005E + geo.B24032._032E

    @censusify
    def manufacturing(geo: Geography):
        return geo.B24032._006E + geo.B24032._033E

    @censusify
    def wholesale_trade(geo: Geography):
        return geo.B24032._007E + geo.B24032._034E

    @censusify
    def retail_trade(geo: Geography):
        return geo.B24032._008E + geo.B24032._035E

    @censusify
    def trans_wareh_util(geo: Geography):
        return geo.B24032._009E + geo.B24032._036E

    @censusify
    def information(geo: Geography):
        return geo.B24032._012E + geo.B24032._039E

    @censusify
    def finance_et_al(geo: Geography):
        return geo.B24032._013E + geo.B24032._040E

    @censusify
    def prof_sci_manag_admin_etc(geo: Geography):
        return geo.B24032._016E + geo.B24032._043E

    @censusify
    def ed_healthcare_etc(geo: Geography):
        return geo.B24032._020E + geo.B24032._047E

    @censusify
    def arts_no_hosp(geo: Geography):
        return (geo.B24032._023E - geo.B24032._025E) + (
            geo.B24032._050E - geo.B24032._052E
        )

    @censusify
    def hosp(geo: Geography):
        return geo.B24032._025E + geo.B24032._052E

    @censusify
    def other_services_except_pub_admin(geo: Geography):
        return geo.B24032._026E + geo.B24032._053E

    @censusify
    def pub_admin(geo: Geography):
        return geo.B24032._027E + geo.B24032._054E

    # Income
    ## Technique: relative bins to median income? or maybe monthly housing costs? other costs?

    @censusify
    def income_under_20k(geo: Geography):
        return sum([geo.B19001._002E, geo.B19001._003E, geo.B19001._004E])

    @censusify
    def income_20k_to_49k(geo: Geography):
        return sum(
            [
                geo.B19001._005E,
                geo.B19001._006E,
                geo.B19001._007E,
                geo.B19001._008E,
                geo.B19001._009E,
                geo.B19001._010E,
            ]
        )

    @censusify
    def income_50k_to_99k(geo: Geography):
        return sum(
            [
                geo.B19001._010E,
                geo.B19001._011E,
                geo.B19001._012E,
                geo.B19001._013E,
            ]
        )

    @censusify
    def income_1000k_to_199k(geo: Geography):
        return sum(
            [
                geo.B19001._014E,
                geo.B19001._015E,
                geo.B19001._016E,
            ]
        )

    population = build_profile(
        indicators=[
            total_population,
        ],
        geographies=[Geography(state="*", county="*")],
        edition=Edition("", "acs5", 2022),
    )

    industry = build_profile(
        indicators=[
            agriculture,
            construction,
            manufacturing,
            wholesale_trade,
            retail_trade,
            trans_wareh_util,
            information,
            finance_et_al,
            prof_sci_manag_admin_etc,
            ed_healthcare_etc,
            arts_no_hosp,
            hosp,
            other_services_except_pub_admin,
            pub_admin,
        ],
        geographies=[Geography(state="*", county="*")],
        edition=Edition("", "acs5", 2022),
    )

    income = build_profile(
        indicators=[
            income_under_20k,
            income_20k_to_49k,
            income_50k_to_99k,
            income_1000k_to_199k,
        ],
        geographies=[Geography(state="*", county="*")],
        edition=Edition("", "acs5", 2022),
    )
    
    demos = pd.concat([
        population.set_index(["geoid", "name"]),
        industry.set_index(["geoid", "name"]),
        income.set_index(["geoid", "name"]),
    ], axis=1).mask(lambda df: df < -9999, 0).reset_index()

    demos.to_csv("demos.csv", index=False)


demos["fips_state"] = demos["geoid"].apply(pad_geoid).str.slice(0, 2)
demos["fips_county"] = demos["geoid"].apply(pad_geoid).str.slice(2)

study_years = {"2016", "2017", "2018", "2019", "2020"}

temps = pd.read_csv("ave_temps.csv", dtype=str).query("year in @study_years")
precip = pd.read_csv("precip.csv", dtype=str).query("year in @study_years")

all_climate = temps.merge(
    precip,
    on=["state_code", "division_number"],
    suffixes=("_temp", "_precip"),
)


county_ncdc_cross = pd.read_csv("counties_crosswalk.csv", dtype=str)
cbsa_county_cross = pd.read_csv("cbsa_crosswalk.csv", dtype=str)

county_ncdc_cross["fips_state"] = county_ncdc_cross["POSTAL_FIPS_ID"].apply(
    lambda val: f"{val:>05}"[:2]
)

county_ncdc_cross["fips_county"] = county_ncdc_cross["POSTAL_FIPS_ID"].apply(
    lambda val: f"{val:>05}"[2:]
)

county_ncdc_cross["ncdc_state"] = county_ncdc_cross["NCDC_FIPS_ID"].apply(
    lambda val: f"{val:>05}"[:2]
)

county_ncdc_cross["ncdc_county"] = county_ncdc_cross["NCDC_FIPS_ID"].apply(
    lambda val: f"{val:>05}"[2:]
)

crossed = cbsa_county_cross[
    ["cbsa_code", "cbsa_title", "state_code", "county_code"]
].merge(
    county_ncdc_cross[
        ["fips_state", "fips_county", "ncdc_state", "ncdc_county"]
    ],
    left_on=["state_code", "county_code"],
    right_on=["fips_state", "fips_county"],
)

with_climate_pop = crossed.merge(
    all_climate.rename(
        columns={"state_code": "ncdc_state", "division_number": "ncdc_county"}
    ),
    on=["ncdc_state", "ncdc_county"],
).merge(
    demos,
    on=["fips_state", "fips_county"],
)

recent_mean = (
    with_climate_pop.astype(
        {
            "jan_value_temp": "float",
            "feb_value_temp": "float",
            "mar_value_temp": "float",
            "apr_value_temp": "float",
            "may_value_temp": "float",
            "june_value_temp": "float",
            "july_value_temp": "float",
            "aug_value_temp": "float",
            "sept_value_temp": "float",
            "oct_value_temp": "float",
            "nov_value_temp": "float",
            "dec_value_temp": "float",
            "jan_value_precip": "float",
            "feb_value_precip": "float",
            "mar_value_precip": "float",
            "apr_value_precip": "float",
            "may_value_precip": "float",
            "june_value_precip": "float",
            "july_value_precip": "float",
            "aug_value_precip": "float",
            "sept_value_precip": "float",
            "oct_value_precip": "float",
            "nov_value_precip": "float",
            "dec_value_precip": "float",
            "agriculture": "float", 
            "construction": "float",
            "manufacturing": "float", 
            "wholesale_trade": "float", 
            "retail_trade": "float", 
            "trans_wareh_util": "float",
            "information": "float", 
            "finance_et_al": "float", 
            "prof_sci_manag_admin_etc": "float",
            "ed_healthcare_etc": "float", 
            "arts_no_hosp": "float", 
            "hosp": "float",
            "other_services_except_pub_admin": "float", 
            "pub_admin": "float", 
            "income_under_20k": "float",
            "income_20k_to_49k": "float", 
            "income_50k_to_99k": "float", 
            "income_1000k_to_199k": "float"
        }
    )
    .groupby("cbsa_code")
    .agg(
        {
            "cbsa_title": "first",
            "jan_value_temp": "mean",
            "feb_value_temp": "mean",
            "mar_value_temp": "mean",
            "apr_value_temp": "mean",
            "may_value_temp": "mean",
            "june_value_temp": "mean",
            "july_value_temp": "mean",
            "aug_value_temp": "mean",
            "sept_value_temp": "mean",
            "oct_value_temp": "mean",
            "nov_value_temp": "mean",
            "dec_value_temp": "mean",
            "jan_value_precip": "mean",
            "feb_value_precip": "mean",
            "mar_value_precip": "mean",
            "apr_value_precip": "mean",
            "may_value_precip": "mean",
            "june_value_precip": "mean",
            "july_value_precip": "mean",
            "aug_value_precip": "mean",
            "sept_value_precip": "mean",
            "oct_value_precip": "mean",
            "nov_value_precip": "mean",
            "dec_value_precip": "mean",
            "total_population": "sum",
            "agriculture": "sum", 
            "construction": "sum",
            "manufacturing": "sum", 
            "wholesale_trade": "sum", 
            "retail_trade": "sum", 
            "trans_wareh_util": "sum",
            "information": "sum", 
            "finance_et_al": "sum", 
            "prof_sci_manag_admin_etc": "sum",
            "ed_healthcare_etc": "sum", 
            "arts_no_hosp": "sum", 
            "hosp": "sum",
            "other_services_except_pub_admin": "sum", 
            "pub_admin": "sum", 
            "income_under_20k": "sum",
            "income_20k_to_49k": "sum", 
            "income_50k_to_99k": "sum", 
            "income_1000k_to_199k": "sum"
        }
    )
    .reset_index()
)

vals = recent_mean[
    [
        "jan_value_temp",
        "feb_value_temp",
        "mar_value_temp",
        "apr_value_temp",
        "may_value_temp",
        "june_value_temp",
        "july_value_temp",
        "aug_value_temp",
        "sept_value_temp",
        "oct_value_temp",
        "nov_value_temp",
        "dec_value_temp",
        "jan_value_precip",
        "feb_value_precip",
        "mar_value_precip",
        "apr_value_precip",
        "may_value_precip",
        "june_value_precip",
        "july_value_precip",
        "aug_value_precip",
        "sept_value_precip",
        "oct_value_precip",
        "nov_value_precip",
        "dec_value_precip",
        "total_population",
        "construction",
        "manufacturing", 
        "wholesale_trade", 
        "retail_trade", 
        "trans_wareh_util",
        "information", 
        "finance_et_al", 
        "prof_sci_manag_admin_etc",
        "ed_healthcare_etc", 
        "arts_no_hosp", 
        "hosp",
        "other_services_except_pub_admin", 
        "pub_admin", 
        "income_under_20k",
        "income_20k_to_49k", 
        "income_50k_to_99k", 
        "income_1000k_to_199k",
    ]
].values

"""
# Cosine similarity -- Does this make sense here even?

residuals = vals - vals.mean(axis=0)
monthly_variance = np.sqrt((residuals * residuals).sum(axis=0) / vals.shape[0])
z_scores = residuals / monthly_variance
similarities = z_scores @ z_scores.T
"""

residuals = vals - vals.mean(axis=0)
monthly_variance = np.sqrt((residuals * residuals).sum(axis=0) / vals.shape[0])
z_scores = residuals / monthly_variance

scaled = (
    z_scores
    * np.array(
        [
            TEMP_WEIGHT,  # jan_value_temp
            TEMP_WEIGHT,  # feb_value_temp
            TEMP_WEIGHT,  # mar_value_temp
            TEMP_WEIGHT,  # apr_value_temp
            TEMP_WEIGHT,  # may_value_temp
            TEMP_WEIGHT,  # june_value_temp
            TEMP_WEIGHT,  # july_value_temp
            TEMP_WEIGHT,  # aug_value_temp
            TEMP_WEIGHT,  # sept_value_temp
            TEMP_WEIGHT,  # oct_value_temp
            TEMP_WEIGHT,  # nov_value_temp
            TEMP_WEIGHT,  # dec_value_temp
            PRECIP_WEIGHT,  # jan_value_precip
            PRECIP_WEIGHT,  # feb_value_precip
            PRECIP_WEIGHT,  # mar_value_precip
            PRECIP_WEIGHT,  # apr_value_precip
            PRECIP_WEIGHT,  # may_value_precip
            PRECIP_WEIGHT,  # june_value_precip
            PRECIP_WEIGHT,  # july_value_precip
            PRECIP_WEIGHT,  # aug_value_precip
            PRECIP_WEIGHT,  # sept_value_precip
            PRECIP_WEIGHT,  # oct_value_precip
            PRECIP_WEIGHT,  # nov_value_precip
            PRECIP_WEIGHT,  # dec_value_precip
            POP_WEIGHT,  # total_population
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INDUSTRY_WEIGHT,
            INCOME_WEIGHT,
            INCOME_WEIGHT,
            INCOME_WEIGHT,
            INCOME_WEIGHT,
        ]
    ).reshape(1, -1)
)

compared = np.linalg.norm(scaled[:, np.newaxis, :] - scaled, axis=2).argsort(
    axis=1
)

pd.concat(
    [
        recent_mean[["cbsa_code", "cbsa_title"]],
        pd.DataFrame(recent_mean["cbsa_title"].values[compared[:, 1:11]]),
    ],
    axis=1,
).to_csv("l2_norm.csv", index=False)
