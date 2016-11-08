import os
import sys
import time
import traceback
from baus import models
from baus import ual
from baus import studio
import pandas as pd
import orca
import socket
import warnings
from baus.utils import compare_summary
import argparse

# Initial options
warnings.filterwarnings("ignore")
pd.set_option('display.float_format', lambda x: '%.3f' % x)

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-i', '--interact', action='store_true', help='Launch in interactive mode')
parser.add_argument('-d', '--display', action='store_true', help='Print stdout to terminal instead of log')
parser.add_argument('--save', type=int, help='Save results to HDF5 files every specified number of years')
parser.add_argument('--start', type=int, help='Start year')
parser.add_argument('--end', type=int, help='End year')
parser.add_argument('--step', type=int, help='Model every x year')
args = parser.parse_args()

# Define defaults and modify with command line arguments
start, end, LOGS, SAVE, EVERY_NTH_YEAR = 2010, 2010, True, False, 1
if args.start:
    start = args.start
if args.end:
    end = args.end
if args.display:
    LOGS = False
if args.save:
    SAVE = True
    orca.add_injectable("save_step", args.save)
if args.interact:
    LOGS = False
    import code
    code.interact(local=locals())
    sys.exit()
if args.step:
    EVERY_NTH_YEAR = args.step

run_num = orca.get_injectable("run_number")
orca.add_injectable("years_per_iter", EVERY_NTH_YEAR)
orca.add_injectable("start_year", start)
orca.add_injectable("end_year", end)

if LOGS:
    print '***The Standard stream is being written to /runs/run{0}.log***' \
        .format(run_num)
    sys.stdout = sys.stderr = open("runs/run%d.log" % run_num, 'w')

# INITIALIZATION

initial_steps = [
    "correct_baseyear_data",
    "ual_initialize_residential_units",
    "ual_match_households_to_units",
    "ual_assign_tenure_to_units"
]

# SIMULATION STEPS

steps = [
    "neighborhood_vars",  # street network accessibility
    "regional_vars",  # road network accessibility
    "ual_rsh_simulate",  # residential sales hedonic for units
    "ual_rrh_simulate",  # residential rental hedonic for units
    "nrh_simulate",  # non-residential rent hedonic
    "ual_assign_tenure_to_new_units",  # (based on higher of predicted price or rent)
    "ual_households_relocation",  # uses conditional probabilities
    "households_transition",
    "ual_reconcile_unplaced_households",  # update building/unit/hh correspondence
    "ual_hlcm_owner_simulate",  # allocate owners to vacant owner-occupied units
    "ual_hlcm_renter_simulate",  # allocate renters to vacant rental units
    "ual_reconcile_placed_households",  # update building/unit/hh correspondence
    "jobs_relocation",
    "jobs_transition",
    "elcm_simulate",
    "ual_update_building_residential_price",  # apply unit prices to buildings
    "price_vars",
    "scheduled_development_events",
    "alt_feasibility",
    "studio_residential_developer",
    "developer_reprocess",
    "retail_developer",
    "studio_office_developer",
    "ual_remove_old_units",  # (for buildings that were removed)
    "ual_initialize_new_units",  # set up units for new residential buildings
    "ual_reconcile_unplaced_households",  # update building/unit/hh correspondence
    "studio_save_tables",  # saves output for visualization
    "topsheet",
    "diagnostic_output",
    "geographic_summary",
    "travel_model_output"
]

itervars = range(start, end + 1)
if not SAVE:
    steps.remove('studio_save_tables')

# RUN STEPS
print "Started run {} at {}".format(run_num, time.ctime())

try:
    orca.run(initial_steps)
    orca.run(steps, iter_vars=itervars)
except Exception as e:
    print traceback.print_exc()
    raise e

print "Finished", time.ctime()
