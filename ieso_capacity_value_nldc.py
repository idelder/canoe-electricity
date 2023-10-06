"""
Calculates CV from average CF during highest net load hours
"""

import pandas as pd
import numpy as np
import ieso_capacity_factors as ieso_cf
import matplotlib.pyplot as pp

vres = ['SOLAR','WIND','HYDRO_ROR','HYDRO_DLY']
cfs = ieso_cf.get_capacity_factors()
cfs.update({'HYDRO_DLY':pd.read_csv('hydro_dly_cf_8760.csv',index_col=0,header=0)['0']})

production = dict()
for vre in vres:
    production.update({vre:cfs[vre]*ieso_cf.get_total_capacity(vre)*1000})

intertie_flow = pd.read_csv('PUB_IntertieScheduleFlowYear_2020.csv', index_col=False, skiprows=4)


def get_capacity_values(new_cap_MW=0, mw_step=1000):


    demand = pd.read_csv('ieso_elc_demand_2020.csv',index_col=0,skiprows=3).rename(columns={'Ontario Demand': 'load'})

    demand['load'] += intertie_flow['net_flow']

    demand['net_load'] = demand['load'].copy()
    for vre in vres:
        demand['net_load'] -= production[vre]


    # LDC - NLDC top 100 hours
    ccs_ldc = dict()
    for vre in vres:

        # Net load without this VRE
        load = demand['net_load'].copy() + production[vre]
        ldc = load.sort_values(ascending=False)

        # Net load with this VRE
        net_load = demand['net_load'].copy() - cfs[vre]*new_cap_MW
        nldc = net_load.sort_values(ascending=False)

        cv = np.mean(ldc[0:100] - nldc[0:100])
        cap = ieso_cf.get_total_capacity(vre) + new_cap_MW/1000

        cc = cv/cap/1000

        ccs_ldc.update({vre:cc})

    # LDC - NLDC top 100 hours
    ccs_marg_ldc = dict()
    for vre in vres:

        # Net load without this VRE marginal capacity
        load = demand['net_load'].copy() - cfs[vre]*(new_cap_MW - mw_step)
        ldc = load.sort_values(ascending=False)

        # Net load with this VRE marginal capacity
        net_load = demand['net_load'].copy() - cfs[vre]*new_cap_MW
        nldc = net_load.sort_values(ascending=False)

        cv = np.mean(ldc[0:100] - nldc[0:100])
        cap = mw_step/1000

        cc = cv/cap/1000

        ccs_marg_ldc.update({vre:cc})


    # CF top 10 hours
    ccs_cf = dict()
    for vre in vres:

        net_load = demand['net_load'].copy() - cfs[vre]*new_cap_MW
        nldc = net_load.sort_values(ascending=False)
        nldc_hours = nldc.index.to_numpy()[0:10]

        cc = np.mean(cfs[vre][nldc_hours])

        ccs_cf.update({vre: cc})

    return ccs_cf, ccs_ldc, ccs_marg_ldc



ccs_cf = dict()
ccs_ldc = dict()
ccs_marg_ldc = dict()

for vre in vres:
    ccs_cf.update({vre: list()})
    ccs_ldc.update({vre: list()})
    ccs_marg_ldc.update({vre: list()})

stop = 10000
step = 1000

n = 0
for new_mw in range(0,stop,step):

    cc_cf, cc_ldc, cc_marg_ldc = get_capacity_values(new_cap_MW=new_mw)

    for vre in vres:
        ccs_cf[vre].append(cc_cf[vre])
        ccs_ldc[vre].append(cc_ldc[vre])
        ccs_marg_ldc[vre].append(cc_marg_ldc[vre])

    n += 1


f = 1 # figure counter
for vre in vres:

    pp.figure(f)
    # pp.ylim([0,0.8])
    f += 1

    pp.plot(range(0,stop,step),ccs_cf[vre],label='CF total/marginal 10H')
    pp.plot(range(0,stop,step),ccs_ldc[vre], label='LDC total 100H')
    pp.plot(range(0,stop,step),ccs_marg_ldc[vre], label='LDC marginal 100H')

    mean_cf = np.mean(cfs[vre])

    pp.plot([0,stop],[mean_cf,mean_cf],'k-', label='Annual CF')
    pp.legend(loc=1)
    pp.title(vre)

pp.show()