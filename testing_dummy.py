"""
For testing code snippets
"""


import pandas as pd
import sqlite3
import numpy as np
from setup import config
import coders_api


_exs, df_existing, date_accessed = coders_api.get_data(end_point='generators')

df_existing = df_existing.loc[df_existing['install_capacity_in_mw'] > 0]

print(df_existing.groupby(['gen_type','province']).min()['install_capacity_in_mw'])