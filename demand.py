"""
Aggregates hourly demand for electricity
Written by Ian David Elder for the CANOE model
"""

import sqlite3
import coders_api
import os
from matplotlib import pyplot as pp
from setup import config



def aggregate_demand():

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()


    # Annual demand projections
    _annual, df_annual, date_accessed = coders_api.get_data(end_point="forecasted_annual_demand")
    config.references['forecasted_annual_demand'] = config.params['coders']['reference'].replace("<date>", date_accessed).replace("<table>", "forecasted_annual_demand")
    df_annual['region'] = [config.region_map[p.lower()] for p in df_annual['province']]
    df_annual.set_index('region', inplace=True)

    # Demand commodity
    dem_comm = config.commodities.loc['demand']


    for region in config.model_regions:

        """
        ##############################################################
            Annual provincial electricity demand
        ##############################################################
        """

        ## Demand
        for period in config.model_periods:

            ann_dem = config.units.loc['demand', 'coders_conv_fact'] * df_annual.loc[region, str(period)]

            curs.execute(f"""REPLACE INTO
                        Demand(regions, periods, demand_comm, demand, demand_units, demand_notes, reference, data_flags, dq_est)
                        VALUES("{region}", {period}, "{dem_comm['commodity']}", {ann_dem}, "({dem_comm['units']})",
                        "provincial electricity demand projection {df_annual.loc[region, 'province']} {period}",
                        "{config.references["forecasted_annual_demand"]}", "coders", 1)""")


        """
        ##############################################################
            Hourly provincial electricity demand
        ##############################################################
        """

        _hourly, df_hourly, date_accessed = coders_api.get_data(end_point="provincial_demand", year=config.params['default_data_year'], province=region)
        dsd_reference = config.params['coders']['reference'].replace("<date>", date_accessed).replace("<table>", "provincial_demand")
        config.references[f"provincial_demand_{region.lower()}"] = dsd_reference

        if df_hourly is None or (len(df_hourly) < 8760):
            print(f"Insufficient hourly demand data available for {region} so DemandSpecificDistribution was skipped.")
            continue

        hourly_dem = df_hourly['demand_MWh'].iloc[0:8760].fillna(method='backfill').to_numpy()
        
        # Apply tolerance and normalise
        hourly_dem[hourly_dem < hourly_dem.mean() * config.params['dsd_tolerance']] = 0
        dsd = hourly_dem / hourly_dem.sum()

        pp.figure()
        pp.plot(dsd)
        pp.title(f"Hourly normalised demand for {region}")
        pp.ylabel("DSD")
        pp.xlabel("Hour of year")


        ## DemandSpecificDistribution
        for h, row in config.time.iterrows():

            curs.execute(f"""REPLACE INTO
                        DemandSpecificDistribution(regions, season_name, time_of_day_name, demand_name, dsd, dsd_notes, reference, data_flags, dq_est)
                        VALUES("{region}", "{row['season']}", "{row['time_of_day']}", "{dem_comm['commodity']}", {dsd[h]},
                        "{config.params['default_data_year']} hourly demand divided by sum of hourly demand for that year",
                        "{dsd_reference}", "coders", 1)""")
            
    
    conn.commit()
    conn.close()

    print(f"Provincial electricity demand data aggregated into {os.path.basename(config.database_file)}\n")



if __name__ == "__main__":

    aggregate()