import requests
import pandas as pd
import io

"""
response = requests.get(
    "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/county-to-climdivs.txt"
)

file = io.BytesIO(response.content)

pd.read_csv(
    file, header=2, delimiter=" ", dtype=str
).to_csv("counties_crosswalk.csv")
"""

response = requests.get(
    "https://www.ncei.noaa.gov/pub/data/cirs/climdiv/climdiv-pcpncy-v1.0.0-20240306"
)

file = io.BytesIO(response.content)

df = pd.read_fwf(
    file,
    header=None,
    widths=[
        2,
        3,
        2,
        4,
        7,
        7,
        7,
        7,
        7,
        7,
        7,
        7,
        7,
        7,
        7,
        7,
    ],
    dtype=str,
)

df.columns = [
    "state_code",
    "division_number",
    "element_code",
    "year",
    "jan_value",
    "feb_value",
    "mar_value",
    "apr_value",
    "may_value",
    "june_value",
    "july_value",
    "aug_value",
    "sept_value",
    "oct_value",
    "nov_value",
    "dec_value",
]

df.to_csv("precip_div.csv", index=False)

