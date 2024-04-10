import pandas as pd
import numpy as np
from scipy.stats import rankdata

import seaborn as sn
import matplotlib.pyplot as plt

temps = pd.read_csv("ave_temps.csv", dtype=str)

f, axes = plt.subplots(3, 4)

for ax, month in zip(axes.flatten(), [
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
    "dec_value"
]):
    aprils = (
        temps[temps["year"].isin({"2018", "2019", "2020"})]
        .astype({month: "float"})
        .groupby(["state_code", "division_number"])[month]
        .mean()
    )

    ax = sn.histplot(aprils, ax=ax)
    f.set_figheight(8)
    f.set_figwidth(14)
    f.subplots_adjust(
        hspace=0.3,
        wspace=0.3,
    )
    f.suptitle("Average monthly temp 2018-2020 by US county")

plt.show()
