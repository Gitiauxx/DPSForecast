import SpatialTables as spd
import pandas as pd

# define function that attaches students to parcels every year
def placeStudents(year, res, coltomerge, p, students, varCount):

    # merge parcels and residential files
    parcelsRes = pd.merge(p, res[coltomerge], on='SCHEDNUM', how='right')
    parcelsRes = parcelsRes[(parcelsRes.CCYRBLT <= year)]

    #  spatial join between students count and parcels:
    GeoStuParcels = spd.sjoin_within(students[students.Year == str(year)], parcelsRes,
                                     ['SCHEDNUM', 'PROPERTY_CLASS', varCount, 'PROP_CLASS', 'CCYRBLT'], how='inner')

    # keep only residential characteristics
    GeoStudentyearRes = GeoStuParcels.drop_duplicates('STUDENTNUM')
    report1 = pd.DataFrame(GeoStudentyearRes.groupby(['PROPERTY_CLASS', 'adjGradeCr', 'NBHD_ID'])[varCount].size())
    report1.columns = ['Students']

    return report1, GeoStuParcels

def StudentsToBuildings(year, realsf, reala, parcels, GeoStudents, coltoMerge, coltoMerge2):

    """
    :param year: year of octber count
    :param realsf: assessor records for single-family/condos/rowhouses
    :param reala: asssessor records for apartment units
    :param parcels: parcels data
    :param students October Cunts
    :param coltoMerge: columns to use from the assessor data for single-family/condos/rowhouses
    :param coltoMerge2: columns to use from the assessor data for apartment units
    :return: students placed in buildings and a count of students by grade, neighborhood and building type
    """

    #Initial placement in the building using student long, lat and parcels polygons
    report1, GeoStuParcels1 = placeStudents(year, realsf, coltoMerge, parcels, GeoStudents, 'UNITS')
    report2, GeoStuParcels2 = placeStudents(year, reala, coltoMerge2, parcels, GeoStudents, 'TOT_UNITS')

    # put together students in single-family/condos/rowhouses and apartment units
    report = pd.concat([report1, report2])

    # summarize built-out environment from assessors table for the year
    resreport2 = pd.DataFrame(reala[reala.ORIG_YOC <= year].groupby(['PROPERTY_CLASS', 'NBHD_ID']).TOT_UNITS.sum())
    resreport2.columns = ['built-out']
    resreport1 = pd.DataFrame(realsf[realsf.CCYRBLT <= year].groupby(['PROPERTY_CLASS', 'NBHD_ID']).UNITS.sum())
    resreport1.columns = ['built-out']
    resreport = pd.concat([resreport1, resreport2]).reset_index().groupby(['PROPERTY_CLASS', 'NBHD_ID']).sum()
    resreport = pd.DataFrame(resreport)
    resreport.columns = ['built-out']

    # adjust buildinbg type into aggregate classifications
    resreport = resreport.reset_index()
    resreport['BUILDING_TYPE'] = resreport.PROPERTY_CLASS
    resreport['NBHD_ID'] = resreport['NBHD_ID'].apply(lambda x: int(x))

    aggClass = ['All Others', 'COMMERCIAL CONDOMINIUM', 'Charitable', 'Commercial - Partially Exempt',
                'County Exempt', 'Exempt - Comm/Resd', 'Mixed Comm/Resd Use',
                'Parsonages', 'Political Subdivisions', 'Religious Worship',
                'Residential Partial Exempt', 'SCHOOLS-PRIVATE-RESD', 'State Exempt']
    resreport.loc[resreport.PROPERTY_CLASS.isin(aggClass), 'BUILDING_TYPE'] = 'Others'
    resreport = resreport.groupby(['BUILDING_TYPE', 'NBHD_ID'])[['built-out']].sum()

    report = report.reset_index()
    report['BUILDING_TYPE'] = report.PROPERTY_CLASS
    report.loc[report.PROPERTY_CLASS.isin(aggClass), 'BUILDING_TYPE'] = 'Others'

    # look for unplaced students
    unplaced = GeoStudents[GeoStudents.Year == str(year)]
    unplaced = unplaced[~unplaced.STUDENTNUM.isin(list(GeoStuParcels1.STUDENTNUM) + list(GeoStuParcels2.STUDENTNUM))]
    print("the initial number of unplaced students is %d for year %d" %(len(unplaced), year))

    # use buffered parcels to place unplaced students
    parcels2 = parcels.copy()
    parcels2.loc[:, 'geometry'] = parcels['geomb']

    # place unplaced students using buffered parcels
    ureport1, unplParcels1 = placeStudents(year, realsf, coltoMerge, parcels2, unplaced, 'UNITS')
    ureport2, unplParcels2 = placeStudents(year, reala, coltoMerge2, parcels2, unplaced, 'TOT_UNITS')

    # combine students in single-family/condos/rowhouses and apartment units
    ureport = pd.concat([ureport1, ureport2])
    ureport = ureport.reset_index()
    ureport['BUILDING_TYPE'] = ureport.PROPERTY_CLASS
    ureport.loc[ureport.PROPERTY_CLASS.isin(aggClass), 'BUILDING_TYPE'] = 'Others'

    # combine the newly placed students with the previous ones
    report = pd.concat([ureport, report])

    # create a count by grade, building type and neighorhood of students
    report = report.groupby(['BUILDING_TYPE', 'adjGradeCr', 'NBHD_ID'])[['Students']].sum()
    report = report.reset_index()
    report['NBHD_ID'] = report['NBHD_ID'].astype('int32')

    yieldsData = pd.merge(report, resreport, left_on=['BUILDING_TYPE', 'NBHD_ID'], right_index=True)

    # remaining unplaced
    unplaced2 = unplaced
    unplaced2 = unplaced2[~unplaced2.STUDENTNUM.isin(list(unplParcels1.STUDENTNUM) + list(unplParcels2.STUDENTNUM))]
    print("the final number of unplaced students is %d for year %d" % (len(unplaced2), year))

    # associate students with a parcel id
    varStudents = ['STUDENTNUM', 'PROP_CLASS', 'CCYRBLT', 'Year']
    StudentsBuildings = pd.concat([GeoStuParcels1[varStudents].drop_duplicates('STUDENTNUM'),
                                   GeoStuParcels2[varStudents].drop_duplicates('STUDENTNUM'),
                                   unplParcels1[varStudents].drop_duplicates('STUDENTNUM'),
                                   unplParcels2[varStudents].drop_duplicates('STUDENTNUM')], axis=0)

    return StudentsBuildings, yieldsData

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