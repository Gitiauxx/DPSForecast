import pandas as pd

def AssessorCounts(p, parcelsForeclosed, reala, realsf):


    # merge assessor tables with parcels tables
    realsf['UNITS_ASS'] = realsf['UNITS']
    reala['UNITS_ASS'] = reala['TOT_UNITS']
    reala['CCYRBLT'] = reala['ORIG_YOC']
    collist = ['PROPERTY_CLASS', 'UNITS_ASS', 'SCHEDNUM', 'CCYRBLT']
    p = p.merge(pd.concat([reala[collist], realsf[collist]]), on='SCHEDNUM')

    # aggregate building type
    p['BUILDING_TYPE'] = p.PROPERTY_CLASS
    aggClass = ['All Others', 'COMMERCIAL CONDOMINIUM', 'Charitable', 'Commercial - Partially Exempt',
                'County Exempt', 'Exempt - Comm/Resd', 'Mixed Comm/Resd Use',
                'Parsonages', 'Political Subdivisions', 'Religious Worship',
                'Residential Partial Exempt', 'SCHOOLS-PRIVATE-RESD', 'State Exempt']
    p.loc[p.index.isin(aggClass), 'BUILDING_TYPE'] = 'Others'


    # group units by building type, year and GEOID
    buildingsYear = p.groupby(['CCYRBLT', 'BUILDING_TYPE', 'GEOID'])[['UNITS_ASS']].sum()

    # adjust schednum (Assessor ID)
    parcelsForeclosed['ScheduleNumber'] = parcelsForeclosed.SCHEDNUM.apply(lambda x: '0' + str(x))
    parcelsForeclosed['len_SchID'] = parcelsForeclosed.ScheduleNumber.apply(lambda x: len(x))
    parcelsForeclosed.loc[parcelsForeclosed['len_SchID'] == 12, 'ScheduleNumber'] = parcelsForeclosed.SCHEDNUM.apply(lambda x: '00' + str(x))
    parcelsForeclosed['SCHEDNUM'] = parcelsForeclosed['ScheduleNumber']

    # count units foreclosed by block group and building type
    # the year of foreclosure is assimilated to a year built
    pfore1 = pd.merge(parcelsForeclosed[['GEOID', 'year', 'SCHEDNUM']], realsf, on='SCHEDNUM')
    pfore2 = pd.merge(parcelsForeclosed[['GEOID', 'year', 'SCHEDNUM']], reala, on='SCHEDNUM')
    resFore = pd.concat([pfore1, pfore2])

    resFore['BUILDING_TYPE'] = resFore.PROPERTY_CLASS
    resFore.loc[p.index.isin(aggClass), 'BUILDING_TYPE'] = 'Others'
    resFore['CCYRBLT'] = resFore['year']
    ForeYears = resFore.groupby(['CCYRBLT', 'BUILDING_TYPE', 'GEOID'])[['UNITS_ASS']].sum()

    # new and foreclosed buildings
    buildingsYear['foreclosed'] = 0
    ForeYears['foreclosed'] = 1

    buildingsYear = pd.concat([buildingsYear, ForeYears]).reset_index()

    return buildingsYear