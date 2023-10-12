import requests
import pandas as pd
import numpy as np
from matplotlib import pyplot as plot
import json
import sqlite3
import os
import tools


url = "http://reports.ieso.ca/public/GenOutputbyFuelHourly/PUB_GenOutputbyFuelHourly_2020.xml"
df = tools.get_file(url)

print(df.head(10))