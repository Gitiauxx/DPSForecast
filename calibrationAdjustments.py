import pandas as pd
import numpy as np

def yieldsAdjustment(yields):

    """

    :param yields: student yields by building_type and grade
    :return: yields after manual adjustments

    """

    # yield adjustments in West Colfax: new buildings do yield students unless they are single family
    yields.loc[(yields.NBHD_ID == 74) & (yields.BUILDING_TYPE != 'Single Family Residential'), 'rate'] = 0

    # yield adjustments in Five Points: new buildings do yield students unless they are single family
    yields.loc[(yields.NBHD_ID == 26) & (yields.BUILDING_TYPE != 'Single Family Residential'), 'rate'] = 0

    # calibrate yields in Green valley Ranch to match observed data
    yields.loc[(yields.NBHD_ID == 28) & (yields.adjGradeCr != '00'), 'rate'] = 0.65 * yields.loc[(yields.NBHD_ID == 28), 'rate']

    # add a yield for rowhouse in Green Valley Ranch, asssumed to be equal to the yield for rowhouses in Stapleton
    yieldsGVR = yields.loc[(yields.NBHD_ID == 60) & (yields.BUILDING_TYPE == 'Rowhouses'), :]
    yieldsGVR.loc[:, 'NBHD_ID'] = 28
    yields = pd.concat([yields, yieldsGVR])

    # add yields for DIA, assumed to be the same as for Green Valley Ranch
    yieldsDIA = yields.loc[(yields.NBHD_ID == 28), :]
    yieldsDIA.loc[:, 'NBHD_ID'] = 23
    yields = yields[yields.NBHD_ID != 23]
    yields = pd.concat([yields, yieldsDIA])

    return yields

def capRateAdjustments(caprate):

    """

    :param capRate: capture rates averaged over three years
    :return: capture rates adjusted manually to improve fit with observed data
    """

    # reduce cap rate in virginia village
    caprate.loc[['08031005104', '08031015500', '08031005200', '08031005102'], 'rate'] = 0.9 * caprate.loc[
        ['08031005104', '08031015500', '08031005200', '08031005102'], 'rate']

    # reduce cap rate in wash virginial vale
    caprate.loc[['08031015300', '08031005001', '08031005002'], 'rate'] = 0.9 * caprate.loc[
        ['08031015300', '08031005001', '08031005002'], 'rate']

    # reduce cap rate in windsor
    caprate.loc[['08031007037', '08031007088', '08031007089'], 'rate'] = 0.9 * caprate.loc[
        ['08031007037', '08031007088', '08031007089'], 'rate']

    # reduce cap rate in skyland
    caprate.loc[['08031003603'], 'rate'] = 0.85 * caprate.loc[['08031003603'], 'rate']

    # increase cap rate in south park hill
    caprate.loc[['08031004201', '08031004202'], 'rate'] = 1.15 * caprate.loc[['08031004201', '08031004202'], 'rate']

    # increase cap rate in hampden south
    caprate.loc[['08031006809', '08031006804', '08031006810'], 'rate'] = 1.05 * caprate.loc[
        ['08031006809', '08031006804', '08031006810'], 'rate']

    # Stapleton North has same capture rate as Stapleton South
    caprate.loc['08031004107', 'rate'] = caprate.loc['08031004106', 'rate']

    # Green Valley Ranch East has same capture rate as Green Valley Ranch West
    caprate.loc['08031008388', 'rate'] = caprate.loc['08031008389', 'rate']

    return caprate


def mobRateAdjustments(mobRate):

    """

    :param mobRate: mobility rates by grade, year and tract
    :return: mobility rate after adjustment to improve calibation
    """

    # trim mobility rate at 1.08 to avoid outliers
    mobRate.loc[(mobRate.adjGradeCr.isin(['02', '03', '05', '06', '04'])) & (mobRate.MR > 1.08), 'MR'] = 1.08

    # trim first grade 1, 9, 4 to avoid outliers -- most of the mob rae are lower than 1
    mobRate.loc[(mobRate.adjGradeCr.isin(['01'])) & (mobRate.MR > 1.0), 'MR'] = 1.0

    # adjust results in specific tract
    mobRate.loc[(mobRate.adjGradeCr.isin(['09', '04'])) & (mobRate.TRACT_ID.isin(['08031008391'])), 'MR'] = 1.0

    # remove extreme values but in a few tracts
    special_tract = [u'08031003201', u'08031003202', u'08031004002']
    mobRate = mobRate[(~np.isnan(mobRate.MR)) & (((mobRate.MR < 1.5) & (mobRate.MR > 0.4)) | (mobRate.TRACT_ID.isin(special_tract)))]

    return mobRate












