import pandas as pd

def mobRate(df):

    df['CountGrade1'] = df.groupby('TRACT_ID').Count1.shift(1)
    return df


def RemoveForeclosed(students, studentsfore):

    """

    :param students: October counts
    :param studentsfore: students living in property foreclosed the year before
    :return: October counts purged of students living in property foreclosed the year before
    """

    students1 = pd.merge(students, studentsfore, on=['STUDENTNUM', 'Year'], how='left')
    students1['FORE_ID'] = students1['FORE_ID'].fillna(0)

    return students1[students1.FORE_ID == 0]

def RemoveMovers(students, StudentsBuildings):

    """

    :param students: October counts
    :param StudentsBuildings: Students attached to a residential parcels
    :param Student living in  a property built the year before
    :return: October counts purged of students living in property built the year before
    """

    students1 = pd.merge(students, StudentsBuildings[['STUDENTNUM', 'Year', 'CCYRBLT', 'PROP_CLASS']],
                         on=['STUDENTNUM', 'Year'], how='inner')

    return students1[students1.CCYRBLT <= students1.Year.astype('int32') - 1]


def ComputeMobRate(students, StudentsBuildings, studentsfore):

    # remove movers and students living in units previously foreclosed
    students1 = RemoveForeclosed(students, studentsfore)
    students2 = RemoveMovers(students1, StudentsBuildings)

    #create all combinations of grades, year and Residential codes
    res_list = list(set(students1.TRACT_ID))
    year_list = list(set(students1.Year))
    grade_list = list(set(students1.adjGradeCr))

    # number of students in the previous cohort
    studentsRes1 = pd.DataFrame(index=pd.MultiIndex.from_product([res_list, grade_list, year_list],
                                                                 names=('TRACT_ID', 'adjGradeCr', 'Year')))
    studentsRes1['Count'] = students.groupby(['TRACT_ID', 'adjGradeCr', 'Year']).size()
    studentsRes1['Count'] = studentsRes1['Count'].fillna(0)
    studentsRes1 = studentsRes1.reset_index()
    studentsRes1 = studentsRes1.sort_values(by=['TRACT_ID', 'adjGradeCr', 'Year'], axis=0)

    # count for the same grade but the year before:
    studentsRes1['Count1'] = studentsRes1.groupby(['TRACT_ID', 'adjGradeCr']).Count.shift(1)
    mobData = studentsRes1.groupby('Year').apply(mobRate)

    # current of students after removing movers
    studentsRes2 = pd.DataFrame(index=pd.MultiIndex.from_product([res_list, grade_list, year_list],
                                                                 names=('TRACT_ID', 'adjGradeCr', 'Year')))
    studentsRes2['Count'] = students2.groupby(['TRACT_ID', 'adjGradeCr', 'Year']).size()
    studentsRes2['Count'] = studentsRes2['Count'].fillna(0)
    studentsRes2 = studentsRes2.reset_index()
    studentsRes2 = studentsRes2.sort_values(by=['TRACT_ID', 'adjGradeCr', 'Year'], axis=0)

    # merge current count and the count of the previous cohort
    mobData = pd.merge(studentsRes2[['Count', 'TRACT_ID', 'adjGradeCr', 'Year']],
                       mobData[['CountGrade1', 'TRACT_ID', 'adjGradeCr', 'Year']],
                       on=['TRACT_ID', 'adjGradeCr', 'Year'])

    # compute mobility rates
    mobData['MR'] = mobData['Count'] / mobData['CountGrade1']

    # remove mobility rate outside of denver county
    mobData['county'] = mobData['TRACT_ID'].apply(lambda x: x[1:5])
    mobData = mobData[mobData.county == '8031']
    mobData['Year'] = mobData.Year.astype('int32')

    return mobData








