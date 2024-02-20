"""
This script gets 8760 transfer flows per regional boundary
Written by Ian David Elder for the TEMOA Canada / CANOE model
"""

import numpy as np
import coders_api
import sqlite3
import os
from setup import config
from utils import string_cleaner



def aggregate_interties():

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()


    """
    ##############################################################
        Determine hourly flows for each intertie in base year
    ##############################################################
    """

    int_tech = config.trans_techs.loc['INTERTIE', 'tech']

    # Regional interfaces
    # Get flows and fix for each boundary
    # Do this up here so it doesn't slam the CODERS database twice for no reason
    intertie_flows = dict()
    for interties, row in config.trans_regions.iterrows():
        
        tech = int_tech + "-" + row['tag']

        # There are multiple interties per some region boundaries so skip duplicates
        if tech in intertie_flows.keys(): continue

        region_1_canoe = config.region_map[row['region_1']]
        region_2_canoe = config.region_map[row['region_2']]

        # Do not represent interties for provinces not included. Note us states always included
        if (region_1_canoe not in config.model_regions) and (region_2_canoe not in config.model_regions): continue

        # Get 8760 transfers from the data year for this boundary and convert MWh to PJ
        from_region_1, from_region_2 = get_transfered_mwh(row['region_1'], row['region_2'], row['type'])
        intertie_flows[tech] = {region_1_canoe: from_region_1, region_2_canoe: from_region_2}


    interfaces, df_interfaces, date_accessed = coders_api.get_data(end_point='interface_capacities')
    config.references['interface_capacities'] = config.params['coders_reference'].replace('<date>', date_accessed)

    interface_techs = dict() # keys are CANOE techs

    elc_comm = config.commodities.loc[config.trans_techs.loc['INTERTIE', 'input_comm']]
    ex_comm = config.commodities.loc[config.trans_techs.loc['INTERTIE', 'output_comm']]

    # Remember that everything here runs twice, regions 1-2 then 2-1
    for idx, row in df_interfaces.iterrows():

        interties = row['associated_interties']

        tech = int_tech + "-" + config.trans_regions.loc[interties, 'tag']

        from_region = config.region_map[row['export_from'].upper()]
        to_region = config.region_map[row['export_to'].upper()]

        # Don't represent interties outside the model or boundary interties with insufficient data
        if (from_region not in config.model_regions) and (to_region not in config.model_regions): continue
        if (from_region in config.model_regions) != (to_region in config.model_regions) and intertie_flows[tech][from_region] is None: continue

        # Prepare some data about this interface
        if tech not in interface_techs.keys():
            interface_techs[tech] = {
                    'description': string_cleaner(interties),
                    'regions': [from_region, to_region],
                    'transfers_from': {from_region: intertie_flows[tech][from_region], to_region: intertie_flows[tech][to_region]},
                    'capacity_from': {from_region: {'summer': 0, 'winter': 0}, to_region: {'summer': 0, 'winter': 0}},
                    'efficiency': 1.0
                }
        elif string_cleaner(interties) not in interface_techs[tech]['description']:
            interface_techs[tech]['description'] += ' - ' + string_cleaner(interties)

        # CODERS gives different capacities for summer/winter and for directions of flow -> capacity factor
        summer_capacity = config.units.loc['capacity', 'conversion_factor'] * row['summer_capacity_mw']
        winter_capacity = config.units.loc['capacity', 'conversion_factor'] * row['winter_capacity_mw']

        # Take the largest of summer/winter capacity then aggregate all interties per region boundary
        interface_techs[tech]['capacity_from'][from_region]['summer'] += summer_capacity
        interface_techs[tech]['capacity_from'][from_region]['winter'] += winter_capacity

    

    """
    ##############################################################
        Add intertie data to database
    ##############################################################
    """

    # Now that data is ready for each interface, add to database
    for tech, interface in interface_techs.items():
        
        # Max capacity is largest of both directions and summer/winter (TEMOA demands a single capacity per intertie)
        max_capacity = max( [max(val.values()) for val in list(interface_techs[tech]['capacity_from'].values())] ) # it works dont mess with it
        if max_capacity <= 0: continue # zero capacity comes up with retired interfaces

        # Some interface flows exceed rated capacity so take max hourly flow as max cap and convert from MWh/h to GW
        # This is for fixed-flow model boundary interfaces
        if (interface['regions'][0] in config.model_regions) != (interface['regions'][1] in config.model_regions):
            max_capacity = max( max(interface['transfers_from'][interface['regions'][0]]), max(interface['transfers_from'][interface['regions'][1]]) )
            max_capacity /= 1000 # MWh/h to GW

        description = interface['description']

        # technologies
        curs.execute(f"""REPLACE INTO
                    technologies(tech, flag, sector, tech_desc)
                    VALUES("{tech}", "p", "electric", "{description}")""")
        
        # tech_exchange
        curs.execute(f"""REPLACE INTO
                    tech_exchange(tech, notes)
                    VALUES("{tech}", "{description}")""")
        
        # tech_curtailment set as flows are fixed
        curs.execute(f"""REPLACE INTO
                    tech_curtailment(tech, notes)
                    VALUES("{tech}", "{description}")""")
        

        # Fill tables for r1-r2 and r2-r1
        for r in [0,1]:

            from_region = interface['regions'][r]
            to_region = interface['regions'][1-r]
            region = from_region + '-' + to_region

            input_comm = ex_comm if from_region not in config.model_regions else elc_comm
            output_comm = ex_comm if to_region not in config.model_regions else elc_comm

            # commodities
            curs.execute(f"""REPLACE INTO
                        commodities(comm_name, flag, comm_desc)
                        VALUES('{input_comm['commodity']}', '{input_comm['flag']}', '{input_comm['description']}')""")
            curs.execute(f"""REPLACE INTO
                        commodities(comm_name, flag, comm_desc)
                        VALUES('{output_comm['commodity']}', '{output_comm['flag']}', '{output_comm['description']}')""")

            # Note describing fixed flow interties
            fixed_flow_note = config.params['intertie_fixed_flow_note'].replace("<year>", str(config.params['default_data_year']))

            if from_region not in config.model_regions and to_region not in config.model_regions: continue # both regions outside model

            # ExistingCapacity
            curs.execute(f"""REPLACE INTO
                        ExistingCapacity(regions, tech, vintage, exist_cap, exist_cap_units, exist_cap_notes, reference, data_flags, dq_est)
                        VALUES("{region}", "{tech}", 2020, "{max_capacity}", "{config.units.loc['capacity', 'units']}",
                        "{description}", "{config.references[from_region+"-"+to_region]}", "coders", 1)""")

            # LifetimeTech
            curs.execute(f"""REPLACE INTO
                        LifetimeTech(regions, tech, life, life_notes)
                        VALUES("{region}", "{tech}", 200, "does not retire")""")
            
            # CapacityToActivity
            curs.execute(f"""REPLACE INTO
                        CapacityToActivity(regions, tech, c2a, c2a_notes)
                        VALUES("{region}", "{tech}", "{config.params['c2a']}", "{config.params['c2a_unit']}")""")
            
            # CapacityFactorTech
            # Endogenous intertie, set summer/winter to/from capacities
            if from_region in config.model_regions and to_region in config.model_regions:

                for h in range(8760):

                    season = config.time.loc[h, 'season']
                    time_of_day = config.time.loc[h, 'time_of_day']
                    summer_winter = config.time.loc[h, 'summer_winter']

                    capacity = interface['capacity_from'][from_region][summer_winter]

                    curs.execute(f"""REPLACE INTO
                                CapacityFactorTech(regions, season_name, time_of_day_name, tech, cf_tech, cf_tech_notes, reference, data_flags, dq_est)
                                VALUES("{region}", "{season}", "{time_of_day}", "{tech}", "{capacity/max_capacity}",
                                "{description}", "{config.references['interface_capacities']}", "coders", 1)""")
            
            # Intertie crosses model boundary, fix hourly flow
            elif (from_region in config.model_regions) != (to_region in config.model_regions):

                for h in range(8760):

                    season = config.time.loc[h, 'season']
                    time_of_day = config.time.loc[h, 'time_of_day']

                    cf = interface['transfers_from'][from_region][h]/1000 / max_capacity # MWh/h to PJ

                    curs.execute(f"""REPLACE INTO
                                CapacityFactorTech(regions, season_name, time_of_day_name, tech, cf_tech, cf_tech_notes, reference, data_flags, dq_est)
                                VALUES("{region}", "{season}", "{time_of_day}", "{tech}", "{cf}", "{fixed_flow_note}", "{config.references[from_region+"-"+to_region]}", "coders", "1")""")
            
            # Efficiency
            curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency)
                        VALUES("{region}", "{input_comm['commodity']}", "{tech}", 2020, "{output_comm['commodity']}", 1)""")


    print(f"CODERS API intertie data aggregated into {os.path.basename(config.database_file)}\n")

    conn.commit()
    conn.close()



# Data for provincial grids
def aggregate_provincial_grids():

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()


    """
    ##############################################################
        Dummy transmission techs
    ##############################################################
    """

    # Transmission techs ELC_TX <--> ELC_DX --> D_ELC
    tx_techs = ["TX_TO_DX", "DX_TO_TX"]
    dummy_techs = ["DX_TO_DEM", "G_TO_TX", "GRPS_TO_TX"]
    for trans_tech in tx_techs + dummy_techs:
        #technologies
        curs.execute(f"""REPLACE INTO
                    technologies(tech, flag, sector, tech_desc)
                    VALUES("{config.trans_techs.loc[trans_tech, 'tech']}", "p", "electric", "Transmission dummy tech")""")

    # Regional parameters
    ca_sys_params, df_sys, date_accessed = coders_api.get_data(end_point='CA_system_parameters')
    df_sys.set_index('province', inplace=True)
    config.references['ca_system_parameters'] = config.params['coders_reference'].replace('<date>', date_accessed)

    for province, row in df_sys.iterrows():

        region = config.region_map[province.upper()]
        if region not in config.model_regions: continue # skip unrepresented provinces

        # PlanningReserveMargin
        reserve_margin = row['reserve_requirements_percent']
        curs.execute(f"""REPLACE INTO
                    PlanningReserveMargin(regions, reserve_margin, reference, data_flags, dq_est)
                    VALUES("{region}", "{reserve_margin}", "{config.references['ca_system_parameters']}", "coders", 1)""")
        
        # Transmission loss techs
        line_loss = row["system_line_losses_percent"]

        for trans_tech in tx_techs + dummy_techs:

            row = config.trans_techs.loc[trans_tech]
            tech = row['tech']
            input_comm = config.commodities.loc[row['input_comm']]
            output_comm = config.commodities.loc[row['output_comm']]

            # commodities
            curs.execute(f"""REPLACE INTO
                        commodities(comm_name, flag, comm_desc)
                        VALUES('{input_comm['commodity']}', '{input_comm['flag']}', '{input_comm['description']}')""")
            curs.execute(f"""REPLACE INTO
                        commodities(comm_name, flag, comm_desc)
                        VALUES('{output_comm['commodity']}', '{output_comm['flag']}', '{output_comm['description']}')""")

            # Eff is line loss for TX <-> DX or 1.0 for dummy techs
            eff = 1.0 - line_loss if trans_tech in tx_techs else 1
            note = "average provincial system line losses" if trans_tech in tx_techs else "dummy tech"

            # Efficiency
            curs.execute(f"""REPLACE INTO
                        Efficiency(regions, input_comm, tech, vintage, output_comm, efficiency, eff_notes, data_flags, dq_est)
                        VALUES("{region}", "{input_comm['commodity']}", "{tech}", {config.model_periods[0]}, "{output_comm['commodity']}", {eff}, "{note}", "coders", 1)""")


    print(f"CODERS API provincial grid data aggregated into {os.path.basename(config.database_file)}\n")

    conn.commit()
    conn.close()



# Gets MWh transferred for each hour of the base year along a given intertie
def get_transfered_mwh(region_1, region_2, intertie_type) -> tuple[np.ndarray, np.ndarray] | None:

    data_year = config.params['default_data_year']

    if intertie_type == 'international': transfers, df_transfers, date_accessed = coders_api.get_data(end_point="international_transfers", year=data_year, province=region_1, us_region=region_2)
    elif intertie_type == 'interprovincial': transfers, df_transfers, date_accessed = coders_api.get_data(end_point="interprovincial_transfers", year=data_year, province1=region_1, province2=region_2)

    if (len(transfers) < 8760):
        print(f"Insufficient transfer data on {region_1}-{region_2}. Try switching the intertie regions.")
        return None
    
    # Add reference in either direction to make things easier
    config.references[f"{config.region_map[region_1]}-{config.region_map[region_2]}"] = config.params['coders_reference'].replace('<date>', date_accessed)
    config.references[f"{config.region_map[region_2]}-{config.region_map[region_1]}"] = config.params['coders_reference'].replace('<date>', date_accessed)
  
    hourly_mwh = np.zeros(8760)

    for h in range(8760):
        mwh = df_transfers.loc[h, 'transfers_MWh']

        if mwh is not None:
            hourly_mwh[h] = mwh

    forward = hourly_mwh.copy()
    forward[forward < 0] = 0

    backward = hourly_mwh.copy()
    backward[backward > 0] = 0
    backward *= -1

    return forward, backward