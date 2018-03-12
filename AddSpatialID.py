import SpatialTables as spd

def AddNeighID(GeoData, GeoNBHD, feature_list):
    return spd.sjoin_within(GeoData, GeoNBHD, feature_list)

def AddBGID(GeoData, GeoBG, feature_list):
    return spd.sjoin_within(GeoData, GeoBG, feature_list)

def AddTRACTID(GeoData):

    """

    :param GeoData: a geo data frame already associated with block group ID (GEOID)
    :return: a geo data frame associated with TRACT ID
    """

    GeoData['TRACT_ID'] = GeoData.GEOID.apply(lambda x: x[:-1])
    return GeoData