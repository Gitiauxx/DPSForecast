from AddSpatialID import AddBGID, AddTRACTID, AddNeighID
from parcelsAssessor import calculateParcels
from studentsYields import StudentsToBuildings
from ForeclosuresStudents import StudentsToForeclosed
from Pipeline import AddPipeline
from UnitsCount import AssessorCounts

import SpatialTables as spd
import pandas as pd
import numpy as np

from DefineDirectory import directory_october_counts, directory_GIS, directory_birth, \
    directory_parcels, directory_assessor, directory_yields, \
    directory_studentsbuildings, directory_foreclosures, directory_units


"""flags to choose which part of the model to run
 (1 = run the submodel, 0 =do not run the submodel) """
YIELDS_FLAG = 0
FORECLOSURE_FLAG = 0
BUILDINGS_FLAG = 0
PIPELINE_FLAG = 1

# load students counts data
print("loading October Counts")
GeoStudents = spd.shape_to_geodata(['STUDENTNUM', 'Year', 'adjGradeCr', 'SCHNUM'], directory_october_counts + 'DPS_OctCt_Historic.shp')
GeoStudents = GeoStudents[~GeoStudents.SCHNUM.astype('int32').isin([902, 750])]

# load neighborhood spatial definition
print("loading neighborhood spatial definition")
GeoNBHD = spd.shape_to_geodata(['NBHD_ID', 'NBRHD_NAME'],
                           directory_GIS + 'census_neighborhood_demographics_2010.shp')

# load block group spatial definition
print("loading block group spatial definition")
bgGeo = spd.shape_to_geodata(['GEOID'], directory_GIS + 'tl_2016_08_bg.shp')

# birth count by block group
print("loading birth counts by block group")
birthBG = pd.read_csv(directory_birth + 'bg_Denver_Births_1995_2016.txt', sep="\t")
birthBG['GEOID'] = birthBG['Block_Grou'].apply(lambda x: '0' + str(x))
birthBG = birthBG.set_index('GEOID')

# Some spatial join to add block, tract and neighborhood ID to students data
print("adding block group, tract and neighborhood ID to students records")
GeoStudents = AddBGID(GeoStudents, bgGeo, ['GEOID'])
GeoStudents = AddTRACTID(GeoStudents)
GeoStudents = AddNeighID(GeoStudents, GeoNBHD, ['NBHD_ID', 'NBRHD_NAME'])

# if needed standardize parcels and assessor file
if YIELDS_FLAG + FORECLOSURE_FLAG + BUILDINGS_FLAG >= 1:

    # load parcels data
    print("loading parcels data")
    parcels = spd.shape_to_geodata(['SCHEDNUM', 'CCYRBLT', 'D_CLASS_CN'], directory_parcels + 'parcels2018.shp')

    # assessor records for single family and condos
    print("loading assessor records -- single family")
    realsf = pd.read_csv(directory_assessor + 'real_property_residential_characteristics2018.csv', encoding='latin-1')

    # assessor records  for apartments
    print("loading assessor records -- apartment")
    reala = pd.read_csv(directory_assessor + 'real_property_apartment_and_commercial_characteristics.csv', encoding='latin-1')

    # land use codes
    print("loading land use codes")
    luCodes = pd.read_csv( directory_assessor + 'land_use_code.csv')

    # cleaning parcels and assessir recors
    print(" Standardizing assessor records")
    parcels, realsf, reala, coltoMerge, coltoMerge2 = calculateParcels(parcels, reala, realsf, luCodes, GeoNBHD)

# Model 1: studentsYields places students into residential buildings
# and compute number of students per building type and neighborhood.
if YIELDS_FLAG == 1:

    # apply placeStudents to each year between 2010 and 2017
    StudentsBuildings_list = []

    for year in np.arange(2017, 2018):
        print(" Running student yields for year %d " %year)
        StudentsBuildings, yieldsData = StudentsToBuildings(year, realsf, reala, parcels, GeoStudents, coltoMerge, coltoMerge2)
        yieldsData['rate'] = yieldsData['Students'] / yieldsData['built-out']

        # export yields for that year
        yieldsData.to_csv(directory_yields + 'report_all_NHD_test_func1_recode' + str(year) + '.csv')

        # export students in buildings for that year
        StudentsBuildings.to_csv(directory_studentsbuildings + 'studentsBuildings_recode' + str(year) + '.csv')

# Model 2: identifying parcels with foreclosure and students living in home that were foreclosed
if FORECLOSURE_FLAG == 1:

    # load foreclosing properties
    foreclosures = spd.shape_to_geodata(['FILENUMBER', 'FCYEAR'], directory_foreclosures + 'foreclosures.shp')

    # loop over years and collect foreclosed properties
    d_list = []
    p_list = []
    for year in np.arange(2008, 2017):
        print(" Running foreclosure procedure for year %d" %year)
        studentsfore1, studentsfore2, pfore = StudentsToForeclosed(year, parcels, foreclosures, GeoStudents, bgGeo)
        d_list.append(studentsfore1)
        d_list.append(studentsfore2)
        p_list.append(pfore)

        # clear memory for maintaining performance
        del pfore
        del studentsfore1
        del studentsfore2

    # concatenate all years
    data = pd.concat(d_list)
    pdata = pd.concat(p_list)

    # export data
    data[['STUDENTNUM', 'Year', 'SCHEDNUM', 'adjGradeCr']].to_csv(directory_foreclosures +'studentsForeclosed_noAPT_test_recode.csv')
    pdata.to_csv(directory_foreclosures + 'UnitsForeclosed_noApt_test_recode.csv')

# Model 3: count build for each year and type
if BUILDINGS_FLAG == 1:

    # load parcels with foreclosed properties
    parcelsForeclosed = pd.read_csv(directory_foreclosures + 'UnitsForeclosed_noApt_test_recode.csv')
    # add block group ID to parcels
    pcentroid = spd.centroid_table(parcels[['SCHEDNUM', 'geometry']])
    p = AddBGID(pcentroid, bgGeo, ['GEOID'])
    buildingsYear = AssessorCounts(p, parcelsForeclosed, reala, realsf)

    # export units counts by block group and year_built
    buildingsYear.to_csv(directory_units + 'buidlingsBlockGroup_test_recode_2018.csv')


#  Model 4: add future units
if PIPELINE_FLAG == 1:

    # load future units
    pipeline = pd.read_csv('Y:\\DPS_Data_temp\\Data\\pipeline\\DenverNewUnits_20172023FNES_072018_Scenarios.csv')
    pipeline = pipeline.fillna(0)

    # load historical buildings -- including foreclosed units
    buildings = pd.read_csv(directory_units + 'buidlingsBlockGroup_test_recode.csv')
    buildings = AddPipeline(pipeline, buildings, scenario="BO_TO")
    buildings.to_csv(directory_units + 'all_buidlingsBlockGroup_test_recode_2030_turn_over.csv')



