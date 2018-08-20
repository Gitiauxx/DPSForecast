import pandas as pd


def AddPipeline(pipeline, buildings, scenario=None):

    # extract scenario if necessary
    if scenario is not None:
        pipeline = pipeline[pipeline.Scenario == scenario]

    # correspondence of types
    pipeline.loc[pipeline.BldgType.isin(['20', '20c']), 'BUILDING_TYPE'] = 'Single Family Residential'
    pipeline.loc[pipeline.BldgType == '26', 'BUILDING_TYPE'] = 'Rowhouses'
    pipeline.loc[pipeline.BldgType == '3', 'BUILDING_TYPE'] = 'Condominium'
    pipeline.loc[pipeline.BldgType == '2', 'BUILDING_TYPE'] = 'Multi Unit (9 and Up)'
    pipeline.loc[pipeline.BldgType == '2c', 'BUILDING_TYPE'] = 'Multi Unit (9 and Up)'

    # adjust field names
    pipeline['foreclosed'] = 0
    pipeline['CCYRBLT'] = pipeline['Year'].astype('int32')
    pipeline['UNITS_ASS'] = pipeline['NewUnits']
    pipeline['GEOID'] = pipeline['GEOID10']

    return pd.concat([buildings,
                       pipeline[['GEOID', 'CCYRBLT', 'UNITS_ASS', 'foreclosed', 'BUILDING_TYPE']]])


if __name__ == '__main__':

    from DefineDirectory import directory_october_counts, directory_GIS, directory_birth, \
        directory_parcels, directory_assessor, directory_yields, \
        directory_studentsbuildings, directory_foreclosures, directory_units

    # load future units
    pipeline = pd.read_csv('Y:\\DPS_Data_temp\\Data\\pipeline\\DenverNewUnits_20172023FNES.csv')
    print(pipeline)
    pipeline = pipeline.fillna(0)

    # load historical buildings -- including foreclosed units
    buildings = pd.read_csv(directory_units + 'buidlingsBlockGroup_test_recode_2018.csv')
    buildings = AddPipeline(pipeline, buildings)
    buildings.to_csv(directory_units + 'all_buidlingsBlockGroup_test_recode_2023.csv')