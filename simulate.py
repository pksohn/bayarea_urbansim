import os
import sys
import time
import traceback
from baus import models
from baus import studio
import pandas as pd
import orca
import socket
import warnings
from baus.utils import compare_summary
import argparse
from slacker import Slacker
from results import slack

# Initial options
warnings.filterwarnings("ignore")
pd.set_option('display.float_format', lambda x: '%.3f' % x)

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-i', '--interact', action='store_true', help='Launch in interactive mode')
parser.add_argument('-d', '--display', action='store_true', help='Print stdout to terminal instead of log')
parser.add_argument('--scen', type=int, help='Scenario number')
parser.add_argument('--save', type=int, help='Save results to HDF5 files every specified number of years')
parser.add_argument('--start', type=int, help='Start year')
parser.add_argument('--end', type=int, help='End year')
parser.add_argument('--step', type=int, help='Model every x year')
args = parser.parse_args()

# Define defaults and modify with command line arguments
start, end, LOGS, SAVE, EVERY_NTH_YEAR, SCENARIO = 2010, 2010, True, False, 1, 0
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
if args.scen:
    orca.add_injectable("scenario", args.scen)
    SCENARIO = orca.get_injectable("scenario")

SLACK = True
MODE = "simulation"
slack = Slacker(slack.token)
host = socket.gethostname()

run_num = orca.get_injectable("run_number")
orca.add_injectable("years_per_iter", EVERY_NTH_YEAR)
orca.add_injectable("start_year", start)
orca.add_injectable("end_year", end)

if LOGS:
    print '***The Standard stream is being written to /runs/run{0}.log***' \
        .format(run_num)
    sys.stdout = sys.stderr = open("runs/run%d.log" % run_num, 'w')


def get_simulation_models(SCENARIO, SAVE):

    models = [
        "neighborhood_vars",            # local accessibility vars
        "regional_vars",                # regional accessibility vars

        "rsh_simulate",                 # residential sales hedonic
        "nrh_simulate",                 # non-residential rent hedonic

        "households_relocation",
        "households_transition",

        "jobs_relocation",
        "jobs_transition",

        "price_vars",

        "scheduled_development_events",  # scheduled buildings additions

        "lump_sum_accounts",             # run the subsidized acct system
        "subsidized_residential_developer_lump_sum_accts",

        "alt_feasibility",

        "residential_developer",
        "developer_reprocess",
        "office_developer",
        "retail_developer",
        "additional_units",

        "hlcm_simulate",                 # put these last so they don't get
        "proportional_elcm",             # start with a proportional jobs model
        "elcm_simulate",                 # displaced by new dev

        "studio_save_tables",
        "topsheet",
        "parcel_summary",
        "building_summary",
        "diagnostic_output",
        "geographic_summary",
        "travel_model_output"
    ]

    if not SAVE:
        models.remove('studio_save_tables')

    # calculate VMT taxes
    if SCENARIO in ["1", "3", "4"]:
        # calculate the vmt fees at the end of the year

        # note that you might also have to change the fees that get
        # imposed - look for fees_per_unit column in variables.py

        if SCENARIO == "3":
            orca.get_injectable("settings")["vmt_res_for_res"] = True

        if SCENARIO == "1":
            orca.get_injectable("settings")["vmt_com_for_res"] = True

        if SCENARIO == "4":
            orca.get_injectable("settings")["vmt_com_for_res"] = True
            orca.get_injectable("settings")["vmt_com_for_com"] = False

            models.insert(models.index("office_developer"),
                          "subsidized_office_developer")

        models.insert(models.index("diagnostic_output"),
                      "calculate_vmt_fees")
        models.insert(models.index("alt_feasibility"),
                      "subsidized_residential_feasibility")
        models.insert(models.index("alt_feasibility"),
                      "subsidized_residential_developer_vmt")

    return models


def run_models(MODE, SCENARIO, SAVE):

    orca.run(["correct_baseyear_data"])

    if MODE == "simulation":

        years_to_run = range(start, end+1, EVERY_NTH_YEAR)
        models = get_simulation_models(SCENARIO, SAVE)
        orca.run(models, iter_vars=years_to_run)

    elif MODE == "estimation":

        orca.run([

            "neighborhood_vars",         # local accessibility variables
            "regional_vars",             # regional accessibility variables
            "rsh_estimate",              # residential sales hedonic
            "nrh_estimate",              # non-res rent hedonic
            "rsh_simulate",
            "nrh_simulate",
            "hlcm_estimate",             # household lcm
            "elcm_estimate",             # employment lcm

        ], iter_vars=[2010])

    elif MODE == "baseyearsim":

        orca.run([

            "neighborhood_vars",            # local accessibility vars
            "regional_vars",                # regional accessibility vars

            "rsh_simulate",                 # residential sales hedonic

            "households_transition",

            "hlcm_simulate",                 # put these last so they don't get

            "geographic_summary",
            "travel_model_output"

        ], iter_vars=[2010])

        for geog_name in ["juris", "pda", "superdistrict", "taz"]:
            os.rename(
                "runs/run%d_%s_summaries_2010.csv" % (run_num, geog_name),
                "output/baseyear_%s_summaries_2010.csv" % geog_name)

    elif MODE == "feasibility":

        orca.run([

            "neighborhood_vars",            # local accessibility vars
            "regional_vars",                # regional accessibility vars

            "rsh_simulate",                 # residential sales hedonic
            "nrh_simulate",                 # non-residential rent hedonic

            "price_vars",
            "subsidized_residential_feasibility"

        ], iter_vars=[2010])

        # the whole point of this is to get the feasibility dataframe
        # for debugging
        df = orca.get_table("feasibility").to_frame()
        df = df.stack(level=0).reset_index(level=1, drop=True)
        df.to_csv("output/feasibility.csv")

    else:

        raise "Invalid mode"

print "Started", time.ctime()
print "Current Scenario : ", orca.get_injectable('scenario').rstrip()


if SLACK:
    slack.chat.post_message(
        '#sim_updates',
        'Starting simulation %d on host %s (scenario: %s)' %
        (run_num, host, SCENARIO), as_user=True)

try:

    run_models(MODE, SCENARIO, SAVE)

except Exception as e:
    print traceback.print_exc()
    if SLACK:
        slack.chat.post_message(
            '#sim_updates',
            'DANG!  Simulation failed for %d on host %s'
            % (run_num, host), as_user=True)
    else:
        raise e
    sys.exit(0)

print "Finished", time.ctime()

if SLACK:
    slack.chat.post_message(
        '#sim_updates',
        'Completed simulation %d on host %s' % (run_num, host), as_user=True)

if MODE == "simulation":

    # copy base year into runs so as to avoid confusion

    import shutil

    for fname in [
        "baseyear_juris_summaries_2010.csv",
        "baseyear_pda_summaries_2010.csv",
        "baseyear_superdistrict_summaries_2010.csv",
        "baseyear_taz_summaries_2010.csv"
    ]:

        shutil.copy(
            os.path.join("output", fname),
            os.path.join("runs", "run{}_".format(run_num) + fname)
        )
