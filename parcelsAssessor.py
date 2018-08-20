import SpatialTables as spd
import pandas as pd
import numpy as np

def calculateParcels(parcels, reala, realsf, luCodes, GeoNBHD):

    """

    :param parcels: parcels data
    :param realsf: assessor records for single-family/condos/rowhouses
    :param reala: asssessor records for apartment units
    :param luCodes: land use codes
    :param GeoNBHD: definition of Denver neighborhoods
    :return: cleaned assessors records and parcels data with normalized columns
    and neighborhood ID
    """

    # geometry for buffer
    parcels.loc[:, 'geomb'] = spd.buffer_geom(np.array(parcels['geometry']), 0.005)

    # add neighborhood to parcels
    pneigh = spd.centroid_table(parcels)
    pneigh = spd.sjoin_within(pneigh, GeoNBHD, ['NBHD_ID'], how='left')

    # residential classification
    resclass = ['SINGLE FAMILY', 'APT W/6 UNITS', 'APT MID-RISE>9 UNT,1-9STY',
        'MIXED USE-HOTEL/RESD', 'APT W/7 UNITS', 'APT W/5 UNITS',
        'REST. W/RESID', 'MIXED USE-MOTEL/RESD', 'CONDOMINIUM',
        'APT W/3 UNITS', 'APT W/2 UNITS', 'ROWHOUSE', 'APT W/4 UNITS',
        'APARTMENT UNIT W/COMM', 'RETAIL CONDO', 'OFFICE CONDO',
        'MOBILE HOME - IMP ONLY', 'PBG LOW-RISE, WALK-UP', 'APT LOW-RISE>9UNT, WALK-UP',
        'PBG ON MORE THAN 1 PARCEL', 'PBG MID-RISE, EL, 1-9 STY', 'GROUP/BOARDING HOME-1 KIT',
        'SENIOR CITIZEN APARTMENT', 'RETAIL W/RESID', 'RETAIL, MULTI', 'SCHOOL',
        'MISC ROWHOUSE IMPS', 'MOTEL - CHAIN MIN AMENITIES', 'MIXED USE-RETAIL/RESD']

    # single family and condos files
    realsf['ScheduleNumber'] = realsf.SCHEDNUM.apply(lambda x: '0' + str(x))
    realsf['len_SchID'] = realsf.ScheduleNumber.apply(lambda x: len(x))
    realsf.loc[realsf['len_SchID'] == 12, 'ScheduleNumber'] = realsf.SCHEDNUM.apply(lambda x: '00' + str(x))
    realsf['SCHEDNUM'] = realsf.ScheduleNumber
    realsf = realsf.set_index('ScheduleNumber')

    # collect columns to merge
    coltoMerge = [i for i in realsf.columns if i not in parcels.columns] + ['SCHEDNUM']

    # add neighborhood id
    realsf = pd.merge(realsf, pneigh[['NBHD_ID', 'SCHEDNUM']], on='SCHEDNUM')

    # get lu codes to re-adjust land due for demolition and already considered vacant
    vacant = [int(i) for i in list(luCodes[luCodes['class'] == 'Vacant Land']['code'])]
    realsf['D_CLASS_CN_ADJ'] = realsf['D_CLASS_CN']
    realsf.loc[(realsf.PROP_CLASS.isin(vacant)) & (realsf.UNITS == 1), 'D_CLASS_CN_ADJ'] = 'SINGLE FAMILY'

    # keep condos, rowhouse and single family only
    realsf = realsf[realsf.PROPERTY_CLASS.isin(['Single Family Residential', 'Rowhouses', 'Mobile Homes', 'Condominium'])
                | (realsf.D_CLASS_CN_ADJ.isin(resclass))]

    # appartment files
    reala['ScheduleNumber'] = reala.SCHEDNUM.apply(lambda x: '0' + str(x))
    reala['len_SchID'] = reala.ScheduleNumber.apply(lambda x: len(x))
    reala.loc[reala['len_SchID'] == 12, 'ScheduleNumber'] = reala.SCHEDNUM.apply(lambda x: '00' + str(x))
    reala['SCHEDNUM'] = reala.ScheduleNumber
    reala = reala.set_index('ScheduleNumber')

    # collect columns to merge
    coltoMerge2 = [i for i in reala.columns if i not in parcels.columns] + ['SCHEDNUM']

    # add neighborhood id
    reala = pd.merge(reala, pneigh[['NBHD_ID', 'SCHEDNUM']], on='SCHEDNUM', how='left')

    # keep condos, rowhouse and single family only
    reala = reala[(reala.PROPERTY_CLASS.isin(['Multi Unit (4-8)', 'Multi Unit (9 and Up)', 'Duplexes/Triplexes']))
              | (reala.D_CLASS_CN.isin(resclass))]

    # keep only record from reala and realsf in the parcels data
    parcels1 = pd.merge(parcels, realsf[['SCHEDNUM']], on='SCHEDNUM', how='inner')
    parcels2 = pd.merge(parcels, reala[['SCHEDNUM']], on='SCHEDNUM', how='inner')
    parcels = pd.concat([parcels1, parcels2])

    return parcels, realsf, reala, coltoMerge, coltoMerge2