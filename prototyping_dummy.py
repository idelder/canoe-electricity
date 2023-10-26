import requests
import pandas as pd
import numpy as np
from matplotlib import pyplot as pp
import json
import sqlite3
import os
import tools
from setup import config
import ieso_capacity_factors as ieso_cf
import coders_api

cfs = ieso_cf.get_capacity_factors()

mean_cfs = dict()
for vre in cfs.keys():
    mean_cfs[vre] = np.mean(cfs[vre])

print(pd.DataFrame(columns=['mean capacity factor'], index=cfs.keys(), data=mean_cfs.values()))