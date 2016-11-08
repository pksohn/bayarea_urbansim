import orca
import pandas as pd
from urbansim_defaults import utils
from urbansim.developer.developer import Developer as dev


@orca.step('studio_save_tables')
def studio_save_tables(households, buildings, parcels, jobs, zones, year, start_year, end_year, save_step):
    """
    This orca step saves intermediate versions of data tables, for developing
    visualization proofs of concept.
    """

    run_num = orca.get_injectable("run_number")

    if ((year - start_year) % save_step == 0) | (year == end_year):

        filename = 'runs/studio_run{}_{}.h5'.format(run_num, year)
        for table in [households, buildings, parcels, jobs, zones]:
            table.to_frame().to_hdf(filename, table.name)


def return_on_cost(df):
    df['return_on_cost'] = df.max_profit.clip(1) / df.total_cost
    return df.return_on_cost.values / df.return_on_cost.sum()


@orca.step()
def studio_residential_developer(feasibility, households, buildings, parcels, year,
                          settings, summary, form_to_btype_func,
                          add_extra_columns_func, parcels_geography,
                          limits_settings, final_year):

    kwargs = settings['residential_developer']

    num_units = dev.compute_units_to_build(
        len(households),
        buildings["residential_units"].sum(),
        kwargs['target_vacancy'])

    targets = []
    typ = "Residential"
    # now apply limits - limits are assumed to be yearly, apply to an
    # entire jurisdiction and be in terms of residential_units or job_spaces
    if typ in limits_settings:

        juris_name = parcels_geography.juris_name.\
            reindex(parcels.index).fillna('Other')

        juris_list = limits_settings[typ].keys()
        for juris, limit in limits_settings[typ].items():

            # the actual target is the limit times the number of years run
            # so far in the simulation (plus this year), minus the amount
            # built in previous years - in other words, you get rollover
            # and development is lumpy

            current_total = parcels.total_residential_units[
                (juris_name == juris) & (parcels.newest_building >= 2010)]\
                .sum()

            target = (year - 2010 + 1) * limit - current_total
            # make sure we don't overshoot the total development of the limit
            # for the horizon year - for instance, in Half Moon Bay we have
            # a very low limit and a single development in a far out year can
            # easily build over the limit for the total simulation
            max_target = (final_year - 2010 + 1) * limit - current_total

            if target <= 0:
                    continue

            targets.append((juris_name == juris, target, max_target, juris))
            num_units -= target

        # other cities not in the targets get the remaining target
        targets.append((~juris_name.isin(juris_list), num_units, None, "none"))

    else:
        # otherwise use all parcels with total number of units
        targets.append((parcels.index == parcels.index,
                        num_units, None, "none"))

    for parcel_mask, target, final_target, juris in targets:

        print "Running developer for %s with target of %d" % \
            (str(juris), target)

        # this was a fairly heinous bug - have to get the building wrapper
        # again because the buildings df gets modified by the run_developer
        # method below
        buildings = orca.get_table('buildings')

        new_buildings = utils.run_developer(
            "residential",
            households,
            buildings,
            "residential_units",
            parcels.parcel_size[parcel_mask],
            parcels.ave_sqft_per_unit[parcel_mask],
            parcels.total_residential_units[parcel_mask],
            feasibility,
            year=year,
            form_to_btype_callback=form_to_btype_func,
            add_more_columns_callback=add_extra_columns_func,
            num_units_to_build=target,
            profit_to_prob_func=return_on_cost,
            **kwargs)

        buildings = orca.get_table('buildings')

        if new_buildings is not None:
            new_buildings["subsidized"] = False

        if final_target is not None and new_buildings is not None:
            # make sure we don't overbuild the target for the whole simulation
            overshoot = new_buildings.net_units.sum() - max_target

            if overshoot > 0:
                index = new_buildings.tail(1).index[0]
                index = int(index)
                # make sure we don't get into a negative unit situation
                overshoot = min(overshoot,
                                buildings.local.loc[index,
                                                    "residential_units"])
                buildings.local.loc[index, "residential_units"] -= overshoot

        summary.add_parcel_output(new_buildings)


@orca.step()
def studio_office_developer(feasibility, jobs, buildings, parcels, year,
                     settings, summary, form_to_btype_func, scenario,
                     add_extra_columns_func, parcels_geography,
                     limits_settings):

    dev_settings = settings['non_residential_developer']

    # I'm going to try a new way of computing this because the math the other
    # way is simply too hard.  Basically we used to try and apportion sectors
    # into the demand for office, retail, and industrial, but there's just so
    # much dirtyness to the data, for instance 15% of jobs are in residential
    # buildings, and 15% in other buildings, it's just hard to know how much
    # to build, we I think the right thing to do is to compute the number of
    # job spaces that are required overall, and then to apportion that new dev
    # into the three non-res types with a single set of coefficients
    all_units = dev.compute_units_to_build(
        len(jobs),
        buildings.job_spaces.sum(),
        dev_settings['kwargs']['target_vacancy'])

    print "Total units to build = %d" % all_units
    if all_units <= 0:
        return

    for typ in ["Office"]:

        print "\nRunning for type: ", typ

        num_units = all_units * float(dev_settings['type_splits'][typ])

        targets = []
        # now apply limits - limits are assumed to be yearly, apply to an
        # entire jurisdiction and be in terms of residential_units or
        # job_spaces
        if year > 2015 and typ in limits_settings:

            juris_name = parcels_geography.juris_name.\
                reindex(parcels.index).fillna('Other')

            juris_list = limits_settings[typ].keys()
            for juris, limit in limits_settings[typ].items():

                # the actual target is the limit times the number of years run
                # so far in the simulation (plus this year), minus the amount
                # built in previous years - in other words, you get rollover
                # and development is lumpy

                current_total = parcels.total_job_spaces[
                    (juris_name == juris) & (parcels.newest_building > 2015)]\
                    .sum()

                target = (year - 2015 + 1) * limit - current_total

                if target <= 0:
                    print "Already met target for juris = %s" % juris
                    print "    target = %d, current_total = %d" %\
                        (target, current_total)
                    continue

                targets.append((juris_name == juris, target, juris))
                num_units -= target

            # other cities not in the targets get the remaining target
            targets.append((~juris_name.isin(juris_list), num_units, "none"))

        else:
            # otherwise use all parcels with total number of units
            targets.append((parcels.index == parcels.index, num_units, "none"))

        for parcel_mask, target, juris in targets:

            print "Running developer for %s with target of %d" % \
                (str(juris), target)
            print "Parcels in play:\n", pd.Series(parcel_mask).value_counts()

            # this was a fairly heinous bug - have to get the building wrapper
            # again because the buildings df gets modified by the run_developer
            # method below
            buildings = orca.get_table('buildings')

            new_buildings = utils.run_developer(
                typ.lower(),
                jobs,
                buildings,
                "job_spaces",
                parcels.parcel_size[parcel_mask],
                parcels.ave_sqft_per_unit[parcel_mask],
                parcels.total_job_spaces[parcel_mask],
                feasibility,
                year=year,
                form_to_btype_callback=form_to_btype_func,
                add_more_columns_callback=add_extra_columns_func,
                residential=False,
                num_units_to_build=target,
                profit_to_prob_func=return_on_cost,
                **dev_settings['kwargs'])

            if new_buildings is not None:
                new_buildings["subsidized"] = False

            summary.add_parcel_output(new_buildings)