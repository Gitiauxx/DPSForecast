import pandas as pd


def yearAdj(year):
    b = int(str(year)[2:4])
    b = b - 5
    if b == 0:
        b = '00'
    elif b == -1:
        b = '99'
    elif b == -2:
        b = '98'
    elif b == -3:
        b = '97'
    elif b == -4:
        b = '96'
    elif b == -5:
        b = '95'
    elif b < 10:
        b = '0' + str(b)
    else:
        b = str(b)

    return b

def forecast(year, mobDataAvg, caprate, buildingsYields, last_obs, birthBG):
    # average birth between year and year -1
    b1 = yearAdj(year)
    b2 = yearAdj(year - 1)
    KEnrollment = birthBG[['Births_' + b1, 'Births_' + b2]].copy()
    KEnrollment['birth'] = (0.75 * KEnrollment['Births_' + b1] + 0.25 * KEnrollment['Births_' + b2])

    # forecast using cap rate
    KEnrollment = KEnrollment.reset_index()
    KEnrollment['TRACT_ID'] = KEnrollment.GEOID
    KEnrollment['TRACT_ID'] = KEnrollment['TRACT_ID'].apply(lambda x: x[:-1])
    KEnrollment = KEnrollment.set_index('GEOID')

    KEnrollment = pd.merge(KEnrollment, caprate[['rate']], left_on='TRACT_ID', right_index=True)

    KEnrollment['Forecast_K'] = KEnrollment['birth'] * KEnrollment['rate']

    # add K students from new buildings (built in the last five years)
    # comment: needs to be 5 years
    newBuildings = buildingsYields[(buildingsYields.CCYRBLT >= year - 5) & (buildingsYields.CCYRBLT <= year)
                                   & (buildingsYields.adjGradeCr == '00')]

    newBuildings.loc[(newBuildings.CCYRBLT >= year - 5) & (newBuildings.CCYRBLT <= year)
                     & (newBuildings.foreclosed == 0), 'CCYRBLT'] = year
    newBuildings.loc[(newBuildings.CCYRBLT <= year) & (newBuildings.CCYRBLT >= year - 5)
                     & (buildingsYields.foreclosed == 1), 'CCYRBLT'] = year
    newBuildings = newBuildings[newBuildings.CCYRBLT == year]

    newBuildings = newBuildings.groupby('GEOID')[['New_Students']].sum()
    KEnrollment = KEnrollment.join(newBuildings[['New_Students']], how='outer')

    KEnrollment['New_Students'] = KEnrollment['New_Students'].fillna(0)
    KEnrollment['Forecast'] = KEnrollment['Forecast_K'].astype(float)
    KEnrollment['Forecast'] = KEnrollment['New_Students'].astype(float) + KEnrollment['Forecast']


    # last observed or simulated years
    Enrollment = last_obs
    Enrollment = Enrollment.reset_index()


    Enrollment['TRACT_ID'] = Enrollment.GEOID.apply(lambda x: x[:-1])
    Enrollment = pd.merge(Enrollment, mobDataAvg, left_on=['TRACT_ID', 'adjGradeCr'], right_index=True, how='left')

    Enrollment = Enrollment.sort_values(by=['GEOID', 'adjGradeCr'], axis=0, ascending=True)
    Enrollment['CountGrade1'] = Enrollment.groupby('GEOID').Count.shift(1)
    Enrollment['Forecast_12'] = Enrollment['CountGrade1'] * Enrollment['MR']
    Enrollment = Enrollment[Enrollment.adjGradeCr != '00']

    Enrollment = Enrollment.set_index(['adjGradeCr', 'GEOID'])

    ## add students in new buildings
    newBuildings2 = buildingsYields[(buildingsYields.CCYRBLT == year) & (buildingsYields.adjGradeCr != '00')]
    newBuildings2 = newBuildings2.groupby(['adjGradeCr', 'GEOID'])[['New_Students']].sum()

    Enrollment = Enrollment.join(newBuildings2[['New_Students']], how='outer')

    Enrollment['New_Students'] = Enrollment['New_Students'].fillna(0)
    Enrollment['Forecast'] = Enrollment['Forecast_12'] + Enrollment['New_Students']

    # combine K and 1 to 12
    Enrollment = Enrollment.reset_index()
    KEnrollment = KEnrollment.reset_index()

    KEnrollment['adjGradeCr'] = '00'
    forecast = pd.concat([KEnrollment[['adjGradeCr', 'GEOID', 'Forecast', 'New_Students']],
                          Enrollment[['adjGradeCr', 'GEOID', 'Forecast', 'New_Students']]])

    return forecast



















