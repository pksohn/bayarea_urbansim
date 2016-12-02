import pandas as pd
import pandana as pdna
from pandana.network import Network
from urbansim.utils import misc


def parcels_geo_new(poi_x, poi_y, tpp_id_value, csv_name, dist=1000.0):
    """

    Parameters
    ----------
    poi_x : Pandas Series
        Series of x coordinates of stations
    poi_y : Pandas Series
        Series of y coordinates of stations
    tpp_id_value : str
        Value assigned in tpp_id column for parcels within given distance of stations
    csv_name : str
        Name of new CSV file
    dist : int or float, optional
        Distance from station parcel should be within to be labeled with tpp_id

    Returns
    -------

    """

    # Load original files
    with pd.HDFStore('./2015_09_01_bayarea_v3.h5', mode='r') as store:
        parcels = store.parcels

    pg = pd.read_csv('02_01_2016_parcels_geography.csv')
    pg = pg.merge(parcels[['geom_id', 'x', 'y']], left_on='geom_id', right_on='geom_id')

    # Load network
    with pd.HDFStore('2015_06_01_osm_bayarea4326.h5') as store:
        edges = store.edges
        nodes = store.nodes

    net = Network(node_x=nodes['x'],
                  node_y=nodes['y'],
                  edge_from=edges['from'],
                  edge_to=edges['to'],
                  edge_weights=edges[['weight']])

    pg['node_id'] = net.get_node_ids(pg.x, pg.y)

    net.init_pois(num_categories=1, max_dist=3000, max_pois=1)
    net.set_pois("tmp", poi_x, poi_y)
    nearest = net.nearest_pois(3000, "tmp", num_pois=1)
    nearest.columns = ['dist']

    pg = pg.merge(nearest, how='left', left_on='node_id', right_index=True)
    pg['within'] = pg.apply(lambda row: 1 if row.dist < dist else 0, axis=1)

    pg.tpp_id = pg.apply(lambda row: tpp_id_value if row.within == 1 else row.tpp_id, axis=1)

    pg.drop(['x', 'y', 'node_id', 'dist', 'within'], axis=1, inplace=True)

    pg.to_csv(csv_name, index=False)


def parcels_geo_std(poi_x, poi_y, tpp_id_value, csv_name, dist=1000.0):
    """

    Parameters
    ----------
    poi_x : Pandas Series
        Series of x coordinates of stations
    poi_y : Pandas Series
        Series of y coordinates of stations
    tpp_id_value : str
        Value assigned in tpp_id column for parcels within given distance of stations
    csv_name : str
        Name of new CSV file
    dist : int or float, optional
        Distance from station parcel should be within to be labeled with tpp_id

    Returns
    -------

    """

    # Load original files
    with pd.HDFStore('./2015_09_01_bayarea_v3.h5', mode='r') as store:
        parcels = store.parcels

    pg = pd.read_csv('02_01_2016_parcels_geography.csv')
    pg = pg.merge(parcels[['geom_id', 'x', 'y']], left_on='geom_id', right_on='geom_id')

    # Load network
    with pd.HDFStore('2015_06_01_osm_bayarea4326.h5') as store:
        edges = store.edges
        nodes = store.nodes

    net = Network(node_x=nodes['x'],
                  node_y=nodes['y'],
                  edge_from=edges['from'],
                  edge_to=edges['to'],
                  edge_weights=edges[['weight']])

    pg['node_id'] = net.get_node_ids(pg.x, pg.y)

    net.init_pois(num_categories=1, max_dist=3000, max_pois=1)
    net.set_pois("tmp", poi_x, poi_y)
    nearest = net.nearest_pois(3000, "tmp", num_pois=1)
    nearest.columns = ['dist']

    pg = pg.merge(nearest, how='left', left_on='node_id', right_index=True)
    pg['within'] = pg.apply(lambda row: 1 if row.dist < dist else 0, axis=1)

    def change_tpp(row):
        higher_tpp_values = ['bart1', 'bart2', 'bart3a', 'lrt1']
        if (row.within == 1) & (row.tpp_id not in higher_tpp_values):
            return tpp_id_value
        else:
            return row.tpp_id

    pg.tpp_id = pg.apply(change_tpp, axis=1)

    pg.drop(['x', 'y', 'node_id', 'dist', 'within'], axis=1, inplace=True)

    pg.to_csv(csv_name, index=False)


def parcels_geo_combined(poi_x_bart, poi_y_bart, poi_x_std, poi_y_std,
                         tpp_id_value_bart, tpp_id_value_std, csv_name, dist=1000.0):

    # Load original files
    with pd.HDFStore('./2015_09_01_bayarea_v3.h5', mode='r') as store:
        parcels = store.parcels

    pg = pd.read_csv('02_01_2016_parcels_geography.csv')
    pg = pg.merge(parcels[['geom_id', 'x', 'y']], left_on='geom_id', right_on='geom_id')

    # Load network
    with pd.HDFStore('2015_06_01_osm_bayarea4326.h5') as store:
        edges = store.edges
        nodes = store.nodes

    net = Network(node_x=nodes['x'],
                  node_y=nodes['y'],
                  edge_from=edges['from'],
                  edge_to=edges['to'],
                  edge_weights=edges[['weight']])

    pg['node_id'] = net.get_node_ids(pg.x, pg.y)

    net.init_pois(num_categories=2, max_dist=3000, max_pois=1)
    net.set_pois("bart", poi_x_bart, poi_y_bart)
    net.set_pois("std", poi_x_std, poi_y_std)

    nearest = net.nearest_pois(3000, "bart", num_pois=1)
    nearest.columns = ['dist_bart']
    pg = pg.merge(nearest, how='left', left_on='node_id', right_index=True)
    pg['within_bart'] = pg.apply(lambda row: 1 if row.dist < dist else 0, axis=1)

    pg.tpp_id = pg.apply(lambda row: tpp_id_value_bart if row.within_bart == 1 else row.tpp_id, axis=1)

    nearest = net.nearest_pois(3000, "std", num_pois=1)
    nearest.columns = ['dist_std']
    pg = pg.merge(nearest, how='left', left_on='node_id', right_index=True)
    pg['within_std'] = pg.apply(lambda row: 1 if row.dist < dist else 0, axis=1)

    def change_tpp(row):
        higher_tpp_values = ['bart1', 'bart2', 'bart3a', 'lrt1']
        if (row.within_std == 1) & (row.tpp_id not in higher_tpp_values):
            return tpp_id_value_std
        else:
            return row.tpp_id

    pg.tpp_id = pg.apply(change_tpp, axis=1)

    pg.drop(['x', 'y', 'node_id', 'dist', 'within'], axis=1, inplace=True)

    pg.to_csv(csv_name, index=False)