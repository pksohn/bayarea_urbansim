#!/usr/bin/env bash

SCRIPT=station_area_metrics.py
NET=./../data/2015_06_01_osm_bayarea4326.h5
STATIONS=./../data/stations_metrics.csv

# Run on baseline data for all station location sets

python ${SCRIPT} --hdf ./../data/2015_09_01_bayarea_v3.h5 --net ${NET} --stations ${STATIONS} --alt 1 --out 'station_area_baseline_alt1.csv'
python ${SCRIPT} --hdf ./../data/2015_09_01_bayarea_v3.h5 --net ${NET} --stations ${STATIONS} --alt 2 --out 'station_area_baseline_alt2.csv'
python ${SCRIPT} --hdf ./../data/2015_09_01_bayarea_v3.h5 --net ${NET} --stations ${STATIONS} --alt 3 --out 'station_area_baseline_alt3.csv'
python ${SCRIPT} --hdf ./../data/2015_09_01_bayarea_v3.h5 --net ${NET} --stations ${STATIONS} --alt 4 --out 'station_area_baseline_alt4.csv'
python ${SCRIPT} --hdf ./../data/2015_09_01_bayarea_v3.h5 --net ${NET} --stations ${STATIONS} --alt 5 --out 'station_area_baseline_alt5.csv'
python ${SCRIPT} --hdf ./../data/2015_09_01_bayarea_v3.h5 --net ${NET} --stations ${STATIONS} --alt 6 --out 'station_area_baseline_alt6.csv'

# Scenario 0, no project, for all station location sets, 2035 (run 26)

python ${SCRIPT} --hdf ./../runs/studio_run26_2035.h5 --net ${NET} --stations ${STATIONS} --alt 1 --out 'station_area_nobuild_scen0_alt1_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run26_2035.h5 --net ${NET} --stations ${STATIONS} --alt 2 --out 'station_area_nobuild_scen0_alt2_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run26_2035.h5 --net ${NET} --stations ${STATIONS} --alt 3 --out 'station_area_nobuild_scen0_alt3_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run26_2035.h5 --net ${NET} --stations ${STATIONS} --alt 4 --out 'station_area_nobuild_scen0_alt4_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run26_2035.h5 --net ${NET} --stations ${STATIONS} --alt 5 --out 'station_area_nobuild_scen0_alt5_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run26_2035.h5 --net ${NET} --stations ${STATIONS} --alt 6 --out 'station_area_nobuild_scen0_alt6_2035.csv'

# Scenario 3, no project, for all station location sets, 2035 (run 28)

python ${SCRIPT} --hdf ./../runs/studio_run28_2035.h5 --net ${NET} --stations ${STATIONS} --alt 1 --out 'station_area_nobuild_scen3_alt1_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run28_2035.h5 --net ${NET} --stations ${STATIONS} --alt 2 --out 'station_area_nobuild_scen3_alt2_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run28_2035.h5 --net ${NET} --stations ${STATIONS} --alt 3 --out 'station_area_nobuild_scen3_alt3_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run28_2035.h5 --net ${NET} --stations ${STATIONS} --alt 4 --out 'station_area_nobuild_scen3_alt4_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run28_2035.h5 --net ${NET} --stations ${STATIONS} --alt 5 --out 'station_area_nobuild_scen3_alt5_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run28_2035.h5 --net ${NET} --stations ${STATIONS} --alt 6 --out 'station_area_nobuild_scen3_alt6_2035.csv'

# Scenario 4, no project, for all station location sets, 2035 (run 27)

python ${SCRIPT} --hdf ./../runs/studio_run27_2035.h5 --net ${NET} --stations ${STATIONS} --alt 1 --out 'station_area_nobuild_scen4_alt1_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run27_2035.h5 --net ${NET} --stations ${STATIONS} --alt 2 --out 'station_area_nobuild_scen4_alt2_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run27_2035.h5 --net ${NET} --stations ${STATIONS} --alt 3 --out 'station_area_nobuild_scen4_alt3_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run27_2035.h5 --net ${NET} --stations ${STATIONS} --alt 4 --out 'station_area_nobuild_scen4_alt4_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run27_2035.h5 --net ${NET} --stations ${STATIONS} --alt 5 --out 'station_area_nobuild_scen4_alt5_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run27_2035.h5 --net ${NET} --stations ${STATIONS} --alt 6 --out 'station_area_nobuild_scen4_alt6_2035.csv'

# Alternative 1 (runs 29 and 30)

python ${SCRIPT} --hdf ./../runs/studio_run29_2035.h5 --net ${NET} --stations ${STATIONS} --alt 1 --out 'station_area_scen0_alt1_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run30_2035.h5 --net ${NET} --stations ${STATIONS} --alt 1 --out 'station_area_scen4_alt1_2035.csv'

# Alternative 2 (runs 35 and 36)

python ${SCRIPT} --hdf ./../runs/studio_run35_2035.h5 --net ${NET} --stations ${STATIONS} --alt 2 --out 'station_area_scen0_alt2_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run36_2035.h5 --net ${NET} --stations ${STATIONS} --alt 2 --out 'station_area_scen4_alt2_2035.csv'

# Alternative 5 (runs 37 and 38)

python ${SCRIPT} --hdf ./../runs/studio_run37_2035.h5 --net ${NET} --stations ${STATIONS} --alt 5 --out 'station_area_scen0_alt5_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run38_2035.h5 --net ${NET} --stations ${STATIONS} --alt 5 --out 'station_area_scen4_alt5_2035.csv'

# Alternative 6 (runs 39 and 40)

python ${SCRIPT} --hdf ./../runs/studio_run39_2035.h5 --net ${NET} --stations ${STATIONS} --alt 6 --out 'station_area_scen0_alt6_2035.csv'
python ${SCRIPT} --hdf ./../runs/studio_run40_2035.h5 --net ${NET} --stations ${STATIONS} --alt 6 --out 'station_area_scen4_alt6_2035.csv'