import numpy as np
import pandas as pd

import SpatialTables as spd
from AddSpatialID import AddBGID, AddTRACTID, AddNeighID
from CaptureRate import ComputeCapturRate
from DefineDirectory import directory_october_counts, directory_GIS, directory_birth, \
    directory_results, directory_yields, \
    directory_studentsbuildings, directory_foreclosures, directory_units
from Forecast import forecast
from SurvivalRate import ComputeMobRate
from calibrationAdjustments import yieldsAdjustment, mobRateAdjustments, capRateAdjustments
from studentsYields_temp import buildingsYields

# global assumptions
year_avg_first = 2014
year_avg_last = 2016
year_forecast0 = 2018
year_forecast1 = 2021

# basic crs
crs0 = {'init': u'epsg:4326'}

# load students counts data
print("loading October Counts")
GeoStudents = spd.shape_to_geodata(['STUDENTNUM', 'Year', 'adjGradeCr', 'SCHNUM'],
                               directory_october_counts + 'DPS_OctCt_Historic.shp')
GeoStudents = GeoStudents[~GeoStudents.SCHNUM.astype('int32').isin([902, 750])]


# load neighborhood spatial definition
print("loading neighborhood spatial definition")
GeoNBHD = spd.shape_to_geodata(['NBHD_ID', 'NBRHD_NAME'],
                           directory_GIS + 'census_neighborhood_demographics_2010.shp')


# load block group spatial definition
print("loading block group spatial definition")
bgGeo = spd.shape_to_geodata(['GEOID'], directory_GIS + 'tl_2016_08_bg.shp')
bgGeo['tract'] = bgGeo['GEOID'].apply(lambda x: x[:-1])
bgGeo['county'] = bgGeo.GEOID.apply(lambda x: x[0:5])
bgGeo = bgGeo[bgGeo.county == '08031']

# birth count by block group
print("loading birth counts by block group")
birthBG = pd.read_csv(directory_birth + 'bg_Denver_Births_1995_2016.txt', sep="\t")
birthBG['GEOID'] = birthBG['Block_Grou'].apply(lambda x: '0' + str(x))
birthBG = birthBG.set_index('GEOID')

# Some spatial join to add block, tract and neighborhood ID to students data
print("adding block group, tract and neighborhood ID to students records")
GeoStudents = AddBGID(GeoStudents, bgGeo, ['GEOID'])
GeoStudents = AddTRACTID(GeoStudents)

# keep only Denver county
GeoStudents['county'] = GeoStudents['TRACT_ID'].apply(lambda x: x[0:5])
GeoStudents = GeoStudents[GeoStudents.county == '08031']

# neighborhood and block group relation
print("correspondence block group and neighborhood")
bgGeo2 = spd.centroid_table(bgGeo)
NeighBG = AddNeighID(bgGeo2, GeoNBHD, ['NBHD_ID'])

#collect all students in buildings for all available years
print("loading all students associated with residential data")
d_list = []
for y in np.arange(2005, 2017):
    StudentsBuildings = pd.read_csv(directory_studentsbuildings + 'studentsBuildings_' + str(y) + '.csv')
    d_list.append(StudentsBuildings)
StudentsBuildings = pd.concat(d_list)
StudentsBuildings['Year'] = StudentsBuildings.Year.astype(str)
StudentsBuildings['STUDENTNUM'] = StudentsBuildings.STUDENTNUM.astype(str)
StudentsBuildings = StudentsBuildings.drop_duplicates(['STUDENTNUM', 'Year'])

# collect students living in foreclosed properties
print("loading foreclosed units")
studentsfore = pd.read_csv(directory_foreclosures + 'studentsForeclosed_noApt.csv')
studentsfore['FORE_ID'] = 1
studentsfore['Year'] = studentsfore['Year'].apply(lambda x: str(x)[0:4])
studentsfore['STUDENTNUM'] = studentsfore['STUDENTNUM'].astype(str)
studentsfore = studentsfore[['FORE_ID', 'STUDENTNUM', 'Year']].drop_duplicates(['STUDENTNUM', 'Year'])

# load and adjust yields
year_yields = 2016
print("loading yields data using year %d for reference" %year_yields)
yields = pd.read_csv(directory_yields + 'report_all_NHD_' + str(year_yields) + '.csv')
yields['rate'] = yields['Students'] / yields['built-out']
yields = yields[yields['built-out'] > 0]

# load units counts by type and block group
print("loading units counts")
buildingsYear = pd.read_csv(directory_units + 'AllbuidlingsBlockGroup.csv')
buildingsYear['GEOID'] = buildingsYear['GEOID'].apply(lambda x: '0' + str(x))
buildingsYear['TRACT_ID'] = buildingsYear['GEOID'].apply(lambda x: x[:-1])
buildings = pd.merge(buildingsYear, NeighBG[['GEOID', 'NBHD_ID']], on='GEOID')
buildings['NBHD_ID'] = buildings['NBHD_ID'].astype('int32')

# adjusting yields for calibration
print("adjusting yields")
yields['NBHD_ID'] = yields['NBHD_ID'].astype('int32')
yields = yieldsAdjustment(yields)

# generate potential students for each building type
print("computing students yielded by unit type, year and neighborhood")
bYields = buildingsYields(buildings, yields)

# compute capture rate
print("Compute capture rates")
year_first = 2005
year_last = 2016

KStudents = GeoStudents[GeoStudents.adjGradeCr == '00']
captureRate = ComputeCapturRate(KStudents, birthBG, bYields, StudentsBuildings, studentsfore, year_first, year_last + 1)

# remove extreme values
special_tract1 = ['08031004002', '08031006804']
special_tract = ['08031003201', '08031003202', '08031004002']

captureRate = captureRate[(~captureRate.rate.isin([np.inf])) &
                          (((captureRate.rate > 0.3) & (captureRate.rate < 1.5))
                           | (captureRate.TRACT_ID.isin(special_tract1)))]

caprate = captureRate[(captureRate.year >= 2014) & (captureRate.year <= 2016)].groupby('TRACT_ID')[['rate']].mean()

# compute survival rates
print("Compute survival rates")
mobData = ComputeMobRate(GeoStudents, StudentsBuildings, studentsfore)

# Adjust survival and capture rate for calibration
print("Adjustments for calibration")
caprate = capRateAdjustments(caprate)


mobData = mobRateAdjustments(mobData)
mobData['year'] = mobData.Year.astype('int32')
mobDataAvg = mobData[(mobData.year >= 2014) & (mobData.year <= 2016)].groupby(['TRACT_ID', 'adjGradeCr'])[['MR']].mean()

# last observed data
last_obs = GeoStudents[GeoStudents.Year == str(year_forecast0 - 1)].groupby(['adjGradeCr', 'GEOID'])[['adjGradeCr']]\
                    .size().to_frame('Count')

# run the forecast
print("running forecast from year %d to year %d" %(year_forecast0, year_forecast1))
f_list = []
for year in np.arange(year_forecast0, year_forecast1 + 1):
    print("Forecasting year " + str(year))

    d = forecast(year, mobDataAvg, caprate, bYields, last_obs, birthBG)
    d['year'] = year
    last_obs = d.groupby(['adjGradeCr', 'GEOID'])[['Forecast']].sum().fillna(0)
    last_obs.columns = ['Count']
    u = last_obs.reset_index()
    f_list.append(d)
results = pd.concat(f_list)
results['Enrollment'] = results['Forecast']

# remove early childhood enrollment
results = results[results.adjGradeCr != 'EC']

# export data to outfile
results[['adjGradeCr', 'GEOID', 'Enrollment', 'year']].to_csv(directory_results + 'forecast_testingSPD_3 .csv')









