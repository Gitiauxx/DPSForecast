import pandas as pd

def buildingsYields(buildings, yields):

    """

    :param buildings: number of units by year built (including future) and block group
    :param yields: student yields by building_type and grade
    :return: Number of students yielded by building type, block group and year
    """

    # keep yields with positive number of units
    yields = yields[yields['built-out'] > 0]

    # mer units and yields
    buildingsYields = pd.merge(yields, buildings, on=['BUILDING_TYPE', 'NBHD_ID'])

    # compute yielded students
    buildingsYields['New_Students'] = buildingsYields['rate'] * buildingsYields['UNITS_ASS']

    return buildingsYields