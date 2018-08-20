import pandas as pd
import numpy as np

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

def countYear(year, Students, Birth, fore):

    # aggregate count by neighborhood
    countNeigh = Students[Students.Year == str(year)].groupby('TRACT_ID')[['adjGradeCr']].size().to_frame('K_Students')

    # get the right birth columns
    b1 = yearAdj(year)
    b2 = yearAdj(year - 1)

    # aggregate birth numbers at the tract level
    Birth = Birth.reset_index()
    Birth['TRACT_ID'] = Birth['GEOID'].apply(lambda x: x[:-1])
    Birth2 = Birth.groupby('TRACT_ID')[['Births_' + b1, 'Births_' + b2]].sum()

    # remove foreclosed students
    fory = fore.reset_index()
    fory = fory[(fory.year >= year - 5) & (fory.year < year)].groupby('TRACT_ID')[['KFUNITS']].sum()

    BirthF = Birth2[['Births_' + b1]].join(fory, how='outer')
    BirthF['KFUNITS'] = BirthF['KFUNITS'].fillna(0)

    Birth2['Births_' + b1 + 'f'] = Birth2['Births_' + b1] - BirthF['KFUNITS']

    fory = fore.reset_index()
    fory = fory[(fory.year >= year - 6) & (fory.year < year - 1)].groupby('TRACT_ID')[['KFUNITS']].sum()


    BirthF = Birth2[['Births_' + b2]].join(fory, how='outer')
    BirthF['KFUNITS'] = BirthF['KFUNITS'].fillna(0)

    Birth2['Births_' + b2 + 'f'] = Birth2['Births_' + b2] - BirthF['KFUNITS']

    # average birth of 2 consecutive years
    Birth2['birth'] = (0.25 * Birth2['Births_' + b2 + 'f'] + 0.75 * Birth2['Births_' + b1 + 'f'])

    # merge with students count
    birthStudents = Birth2[['birth']].join(countNeigh, how='inner')

    return birthStudents

def RemoveMovers(KStudents, StudentsBuildings, studentsfore):

    """

    :param KStudents: October Counts for Kinders
    :param StudentsBuildings: Students attached to a residential parcels
    :param studentsfore: Student living in  a property foreclosed within 5 years
    :return: October Counts for Kinders purged of movers in "new" units
    """

    # remove students living in units built within 5 years
    KStudents1 = pd.merge(KStudents,
                          StudentsBuildings[['STUDENTNUM', 'Year', 'CCYRBLT', 'PROP_CLASS']],
                          on=['STUDENTNUM', 'Year'])
    print(KStudents1[(KStudents1.TRACT_ID == '08031008390')].groupby('Year').size())
    KStudents2 = KStudents1[KStudents1.CCYRBLT <= KStudents1.Year.astype(float) - 6]

    # remove students living in a property foreclosed within 5 years
    studentsfore['STUDENTNUM'] = studentsfore['STUDENTNUM'].astype(str)
    KStudents3 = pd.merge(KStudents2, studentsfore, on=['STUDENTNUM', 'Year'], how='left')
    KStudents3['FORE_ID'] = KStudents3['FORE_ID'].fillna(0)
    print(KStudents3[(KStudents3.FORE_ID == 1) & (KStudents3.TRACT_ID == '08031008390')].groupby('Year').size())

    KStudents3 = KStudents3[KStudents3.FORE_ID == 0]

    return KStudents3

def ForeclosedBirth(buildingsYields):

    """

    :param buildingsYields: buildings the number of students they can yield by grade
    :return: number of kinders that could have potentially displaced by year and tract
    """

    ForeYears = buildingsYields[buildingsYields.foreclosed == 1]
    ForeYears.loc[:, 'year'] = ForeYears['CCYRBLT']
    ForeYears.loc[:, 'KFUNITS'] = ForeYears['New_Students'].astype('int32')

    return ForeYears.groupby(['year', 'TRACT_ID'])[['KFUNITS']].sum()


def ComputeCapturRate(KStudents, birthBG, buildingsYields, StudentsBuildings, studentsfore, year_first, year_last):

    """

    :param KStudents: October Counts for kinders purged of movers in "new" units
    :param birthBG: birth counts by block group
    :param birthForeclosed: number of foreclosed properties that cou
    :param year_first:  first year to compute capture rate
    :param year_last: last year to compute capture rate
    :return: capture rate by tract id, grade and year
    """

    # estimate of birth that did not turn into kinders because of foreclosing
    birthForeclosed = ForeclosedBirth(buildingsYields)

    # remove movers
    KStudents = RemoveMovers(KStudents, StudentsBuildings, studentsfore)

    # loop from year_first to year_last
    result_list = []
    for year in np.arange(year_first, year_last):
        result = countYear(year, KStudents, birthBG, birthForeclosed)
        result['year'] = year
        result_list.append(result)

    # concatenate all years
    captureRate = pd.concat(result_list)

    # compute the rate by divinding kinders count by birth (lagged)
    captureRate.index.name = 'TRACT_ID'
    captureRate = captureRate.reset_index()
    captureRate['rate'] = captureRate['K_Students'] / captureRate['birth']

    return captureRate








