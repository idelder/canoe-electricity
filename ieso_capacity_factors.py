"""
Gets aggregate 2020 electricity production by fuel type from IESO data
to calculate 8760 capacity factors
Written by Ian David Elder for the CANOE model
"""

import os
import sqlite3
import numpy as np
import pandas as pd
from setup import config
import tools

params = config.params
translator = config.translator


data_year = params['default_data_year']

this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
translation_file = this_dir + "CODERS_CANOE_translation.sqlite"
coders_db = this_dir + "coders_db.sqlite"

ieso_data = this_dir + "ieso_data/"
cache = ieso_data +  f"ieso_gen_hourly_{data_year}.txt"

show_plots=False

tech_like = dict({
        'WIND_ONSHORE':'%WND_ON%',
        'SOLAR_PV':'%SOL_PV%',
        'HYDRO_RUN':'%HYD_ROR%',
        'HYDRO_DAILY':'%HYD_DLY%',
        'HYDRO':'%HYD%'
        })


def get_ieso_production():

    data = tools.get_file(f"http://reports.ieso.ca/public/GenOutputbyFuelHourly/PUB_GenOutputbyFuelHourly_{data_year}.xml")

    fuels = ['NUCLEAR', 'GAS', 'HYDRO', 'WIND', 'SOLAR', 'BIOFUEL']
    hourly_production = dict()

    for fuel in fuels:
        hourly_production[fuel] = list()

        for day in range(365):
            for hour in range(24):

                # Data was originally in a horribly nested xml format like:
                # data['Document']['DocBody']['DailyData'][day 1-366]['HourlyData'][hour 1 - 24]['FuelTotal'][any vals where val['Fuel'] == fuel]['EnergyValue']['Output']
                hour_data = data['Document']['DocBody']['DailyData'][day]['HourlyData'][hour]['FuelTotal']
                fuel_values = [values for values in hour_data if values['Fuel'] == fuel]
                fuel_value = float(fuel_values[0]['EnergyValue']['Output'])
                hourly_production[fuel].append(fuel_value)

    return hourly_production



def get_capacity_factors():

    hourly_production = get_ieso_production()

    wind_total_cap = get_total_capacity('WIND_ONSHORE')
    solar_total_cap = get_total_capacity('SOLAR_PV')
    hydro_total_cap = get_total_capacity('HYDRO')

    wind_cf = np.array(hourly_production['WIND']) / wind_total_cap / 1000 # MWh/GW.h to PJ/PJ
    solar_cf = np.array(hourly_production['SOLAR']) / solar_total_cap / 1000
    hydro_cf = np.array(hourly_production['HYDRO']) / hydro_total_cap / 1000
    hydro_ror_cf = np.array(pd.read_csv(ieso_data + 'hydro_ror_cf.csv',index_col=0,header=0)['0'])
    hydro_dly_cf = np.array(pd.read_csv(ieso_data + 'hydro_dly_cf_365.csv',index_col=0,header=0)['0']) * 3600 * 24 / 10**6 # GWd to PJ

    return dict({
        'WIND_ONSHORE':wind_cf,
        'SOLAR_PV':solar_cf,
        'HYDRO_RUN':hydro_ror_cf,
        'HYDRO_DAILY':hydro_dly_cf,
        'HYDRO':hydro_cf
        })



def get_total_capacity(vre_type):
    
    conn = sqlite3.connect(coders_db)
    curs = conn.cursor()

    cap = sum([gen[0] for gen in curs.execute(f"SELECT exist_cap FROM ExistingCapacity WHERE regions == 'ON' AND tech LIKE '{tech_like[vre_type]}'").fetchall()])

    conn.close()

    return cap



def write_to_coders_db():

    cfs = get_capacity_factors()

    # Hydro daily storage uses a daily energy allotment (Min/MaxSeasonalActivity)
    hydro_dly_total_cap = get_total_capacity('HYDRO_DAILY')
    hydro_dly_seas_act = cfs['HYDRO_DAILY'] * hydro_dly_total_cap

    conn = sqlite3.connect(coders_db)
    curs = conn.cursor()

    for vre in ['HYDRO_RUN', 'SOLAR_PV', 'WIND_ONSHORE']:

        # Get all variants of this vre tech
        techs = [tech[0] for tech in curs.execute(f"SELECT tech FROM technologies WHERE tech like '{tech_like[vre]}'").fetchall()]

        for tech in techs:
            # Do in this order to keep tech rows together in the database
            for h in range(8760):
                curs.execute(f"""REPLACE INTO
                            CapacityFactorTech(regions, season_name, time_of_day_name, tech, cf_tech)
                            VALUES('ON', '{config.seas_8760[h]}', '{config.tofd_8760[h]}', '{tech}', {cfs[vre][h]})""")


    hyd_dly_variants = [tech[0] for tech in curs.execute(f"SELECT tech FROM technologies WHERE tech like '{tech_like['HYDRO_DAILY']}'").fetchall()]

    for tech in hyd_dly_variants:
        for period in config.model_periods:
            for d in range(365):
                curs.execute(f"""REPLACE INTO
                            MinSeasonalActivity(regions, periods, season_name, tech, minact, minact_units)
                            VALUES('ON', {period}, '{config.seas_8760[d]}', '{tech}', {hydro_dly_seas_act[d]}, 'PJ')""")
                curs.execute(f"""REPLACE INTO
                            MaxSeasonalActivity(regions, periods, season_name, tech, maxact, maxact_units)
                            VALUES('ON', {period}, '{config.seas_8760[d]}', '{tech}', {hydro_dly_seas_act[d]}, 'PJ')""")

    conn.commit()
    conn.close()