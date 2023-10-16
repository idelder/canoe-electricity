"""
Builds the Ontario database
Written by Ian David Elder for the CANOE model
"""

import CODERS_pull
import ieso_capacity_credits as ieso_cc
import ieso_capacity_factors as ieso_cf

ieso_cf.write_to_coders_db()
ieso_cc.write_to_coders_db()