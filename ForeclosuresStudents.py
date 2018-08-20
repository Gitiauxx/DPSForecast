import SpatialTables as spd

def StudentsToForeclosed(year, parcels, foreclosures, GeoStudents, bgGeo):

    # keep foreclosed properties for that year
    fore = foreclosures[foreclosures.FCYEAR == str(year)]

    # residential property with home ownership
    resclass = ['SINGLE FAMILY', 'ROWHOUSE']

    # merge parcels and foreclosed properties
    p = parcels[(parcels.CCYRBLT <= year) & (parcels.D_CLASS_CN.isin(resclass))]
    fore = spd.sjoin_within(fore, p, ['SCHEDNUM'], how='inner')
    fore = fore.drop_duplicates('FILENUMBER')

    # flag parcels with a foreclosed property
    pfore = p[p.SCHEDNUM.isin(fore.SCHEDNUM)]
    pfore.loc[:, 'year'] = year

    # for K, flag students that lived in the foreclosed homes or will move in 5 year
    # after foreclosure
    GeoStudents['Year'] = GeoStudents['Year'].astype('int32')
    studentsfore1 = spd.sjoin_within(GeoStudents[(GeoStudents.Year >= year)
                                          & ((GeoStudents.Year <= year + 5))
                                          & (GeoStudents.adjGradeCr == '00')], pfore, ['SCHEDNUM'], how='inner')



    # for grade 1 to 12 flag students that lived in the foreclosed homes or will move in the year
    # after foreclosure
    studentsfore2 = spd.sjoin_within(GeoStudents[(GeoStudents.Year >= year)
                                          & ((GeoStudents.Year <= year + 1))
                                          & (GeoStudents.adjGradeCr != '00')], pfore, ['SCHEDNUM'], how='inner')

    # add block group id  to parcels with foreclosed property so that we can track their number by block
    # group
    pfore = spd.centroid_table(pfore)
    pfore = spd.sjoin_within(pfore, bgGeo, ['GEOID'], how='left')

    return studentsfore1, studentsfore2, pfore