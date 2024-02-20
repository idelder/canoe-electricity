"""
Performs some final post-processing on aggregated data
Written by Ian David Elder for the CANOE model
"""

import sqlite3
from setup import config



def aggregate_post():

    conn = sqlite3.connect(config.database_file)
    curs = conn.cursor()


    """
    ##############################################################
        Some final input file constraints
    ##############################################################
    """

    # TODO could probably generalise this step to all constraint tables
    # MaxCapacity
    for region in config.model_regions:
        for period in config.model_periods:

            for tech in config.cap_limits[region].index:

                max_cap = float(config.cap_limits[region].loc[tech, period]) * config.units.loc['capacity', 'conversion_factor']
                note = config.cap_limits[region].loc[tech, 'note']
                reference = config.cap_limits[region].loc[tech, 'reference']
                if str(reference) == 'nan': reference = ''
                dq_est = config.cap_limits[region].loc[tech, 'dq_est']

                if dq_est > 0:
                    curs.execute(f"""REPLACE INTO
                                MaxCapacity(regions, periods, tech, maxcap, maxcap_units, maxcap_notes, reference, dq_est)
                                VALUES('{region}', {period}, '{tech}', {max_cap}, '{config.units.loc['capacity', 'units']}',
                                "{note}", "{reference}", {dq_est})""")
                else:
                    curs.execute(f"""REPLACE INTO
                                MaxCapacity(regions, periods, tech, maxcap, maxcap_units, maxcap_notes, reference)
                                VALUES('{region}', {period}, '{tech}', {max_cap}, '{config.units.loc['capacity', 'units']}',
                                "{note}", "{reference}")""")



    """
    ##############################################################
        References
    ##############################################################
    """

    for ref in config.references.values():
        curs.execute(f"""REPLACE INTO
                    'references'('reference')
                    VALUES('{ref}')""")


    conn.commit()
    conn.close()