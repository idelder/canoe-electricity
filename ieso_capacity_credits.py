"""
Calculates capacity credits from IESO demand data
and calculated 8760 capacity factor data
Written by Ian David Elder for the CANOE model
"""

import pandas as pd
import numpy as np
import ieso_capacity_factors as ieso_cf
import matplotlib.pyplot as pp
import sqlite3
import os
import tools



data_year = 2020
this_dir = os.path.realpath(os.path.dirname(__file__)) + "/"
ieso_data = this_dir + "ieso_data/"


vres = ['WIND','SOLAR']
cfs = ieso_cf.get_capacity_factors() # gets hydro daily as 365 days
cfs.update({'HYDRO_DLY':pd.read_csv(ieso_data + 'hydro_dly_cf_8760.csv',index_col=0,header=0)['0']})

production = dict()
for vre in vres:
    production.update({vre:cfs[vre]*ieso_cf.get_total_capacity(vre)*1000})

intertie_flow = tools.get_csv(f"http://reports.ieso.ca/public/IntertieScheduleFlowYear/PUB_IntertieScheduleFlowYear_{data_year}.csv", index_col=False, skiprows=4)



def get_capacity_values(new_mw=0, mw_step=1000):

    demand = pd.read_csv(ieso_data + 'ieso_elc_demand_2020.csv',index_col=0,skiprows=3).rename(columns={'Ontario Demand': 'load'})

    demand['load'] += intertie_flow['net_flow']

    demand['net_load'] = demand['load'].copy()
    for vre in vres:
        demand['net_load'] -= production[vre]



    # Total CC
    # LDC - NLDC top 100 hours
    ccs_ldc = dict()
    for vre in vres:

        # Net load without this VRE
        load = demand['net_load'].copy() + production[vre]
        ldc = load.sort_values(ascending=False)

        # Net load with this VRE
        net_load = demand['net_load'].copy() - cfs[vre]*new_mw
        nldc = net_load.sort_values(ascending=False)

        cv = np.mean(ldc[0:100] - nldc[0:100])
        cap = ieso_cf.get_total_capacity(vre) + new_mw/1000

        cc = cv/cap/1000 # cap in GW in database

        ccs_ldc.update({vre:cc})


    # Marginal CC
    # LDC - NLDC top 100 hours
    ccs_marg_ldc = dict()
    for vre in vres:

        # Net load without this VRE marginal capacity
        load = demand['net_load'].copy() - cfs[vre]*(new_mw - mw_step)
        ldc = load.sort_values(ascending=False)

        # Net load with this VRE marginal capacity
        net_load = demand['net_load'].copy() - cfs[vre]*new_mw
        nldc = net_load.sort_values(ascending=False)

        cv = np.mean(ldc[0:100] - nldc[0:100])
        marg_cap = mw_step/1000

        cc = cv/marg_cap/1000 # capacity in GW in database

        ccs_marg_ldc.update({vre:cc})

    pp.figure(10)
    pp.plot(range(8760),ldc)
    pp.plot(range(8760),nldc)

    # Total / marginal CC
    # CF top 10 hours
    ccs_cf = dict()
    for vre in vres:

        net_load = demand['net_load'].copy() - cfs[vre]*new_mw
        nldc = net_load.sort_values(ascending=False)
        nldc_hours = nldc.index.to_numpy()[0:10]

        cc = np.mean(cfs[vre][nldc_hours])

        ccs_cf.update({vre: cc})


    return ccs_cf, ccs_ldc, ccs_marg_ldc



def get_cc_curves(new_mw_step=1000, new_mw_max=11000, return_type='marg_ldc'):

    new_mws = range(0,new_mw_max,new_mw_step)
    mw_indexed = {'new_mw':list(new_mws)}

    ccs_cf = mw_indexed.copy()
    ccs_ldc = mw_indexed.copy()
    ccs_marg_ldc = mw_indexed.copy()

    for vre in vres:
        ccs_cf.update({vre: list()})
        ccs_ldc.update({vre: list()})
        ccs_marg_ldc.update({vre: list()})

    for new_mw in new_mws:

        cc_cf, cc_ldc, cc_marg_ldc = get_capacity_values(new_mw=new_mw)

        for vre in vres:
            ccs_cf[vre].append(cc_cf[vre])
            ccs_ldc[vre].append(cc_ldc[vre])

            ccs_marg_ldc[vre].append(cc_marg_ldc[vre] if new_mw > 0 else cc_ldc[vre])


    f = 1 # figure counter
    for vre in vres:

        pp.figure(f)
        # pp.ylim([0,0.8])
        f += 1

        pp.plot(ccs_cf['new_mw'],ccs_cf[vre],label='CF total/marginal 10H')
        pp.plot(ccs_ldc['new_mw'],ccs_ldc[vre], label='LDC total 100H')
        pp.plot(ccs_marg_ldc['new_mw'],ccs_marg_ldc[vre], label='LDC marginal 100H')

        mean_cf = np.mean(cfs[vre])

        pp.plot([0,new_mw_max],[mean_cf,mean_cf],'k-', label='Annual CF')
        pp.legend(loc=1)
        pp.title(vre)


    to_return = {
        'cf': ccs_cf,
        'tot_ldc': ccs_ldc,
        'marg_ldc': ccs_marg_ldc
    }

    return to_return[return_type]



ccs = get_cc_curves(return_type='marg_ldc')
ccs['new_mw'][0] = 'existing'

df = pd.DataFrame(ccs)
df.to_csv(ieso_data + 'ieso_capacity_credits.csv')

print(df.head(len(ccs['new_mw'])))

pp.show()

# TODO fill this function
def write_to_coders_db(download=False, update_cache=False):

    cfs = get_capacity_factors(download=download, update_cache=update_cache)

    hydro_dly_total_cap = get_total_capacity('HYDRO_DLY')
    hydro_dly_seas_act = cfs['HYDRO_DLY'] * hydro_dly_total_cap

    conn = sqlite3.connect(coders_db)
    curs = conn.cursor()

    # Run these separately to keep the database in order
    for h in range(8760):
        curs.execute(f"""REPLACE INTO
                    CapacityFactorTech(regions, season_name, time_of_day_name, tech, cf_tech)
                    VALUES('ON', '{DDD(seas_8760[h])}', '{HH(tofd_8760[h])}', 'E_WND_ON', {cfs['WIND'][h]})""")
    for h in range(8760):
        curs.execute(f"""REPLACE INTO
                    CapacityFactorTech(regions, season_name, time_of_day_name, tech, cf_tech)
                    VALUES('ON', '{DDD(seas_8760[h])}', '{HH(tofd_8760[h])}', 'E_SOL_PV', {cfs['SOLAR'][h]})""")
    for h in range(8760):
        curs.execute(f"""REPLACE INTO
                    CapacityFactorTech(regions, season_name, time_of_day_name, tech, cf_tech)
                    VALUES('ON', '{DDD(seas_8760[h])}', '{HH(tofd_8760[h])}', 'E_HYD_ROR', {cfs['HYDRO_ROR'][h]})""")
        
    for period in range(2020,2055,5):
        for d in range(365):
            curs.execute(f"""REPLACE INTO
                        MinSeasonalActivity(regions, periods, season_name, tech, minact, minact_units)
                        VALUES('ON', {period}, '{DDD(seas_8760[d])}', 'E_HYD_DLY', {hydro_dly_seas_act[d]}, 'PJ')""")
            curs.execute(f"""REPLACE INTO
                        MaxSeasonalActivity(regions, periods, season_name, tech, maxact, maxact_units)
                        VALUES('ON', {period}, '{DDD(seas_8760[d])}', 'E_HYD_DLY', {hydro_dly_seas_act[d]}, 'PJ')""")

    conn.commit()
    conn.close()