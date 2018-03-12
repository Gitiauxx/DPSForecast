import os

# main directory
main_directory = 'Y:\\DPS_Data_temp\\Data'


# directory for yields
directory_yields = os.path.join(main_directory, 'yields\\')

# directory for October counts
directory_october_counts = os.path.join(main_directory, 'Students2017\\')

# GIS directory (neighborhoods,
# block group and tract)
directory_GIS = os.path.join(main_directory, 'boundaries\\')

# birth directory
directory_birth = os.path.join(main_directory, 'Fertility\\')

# parcels and assessor data
directory_parcels = os.path.join(main_directory, 'assessor\\')
directory_assessor = os.path.join(main_directory, 'assessor\\')

# directory for pipeline projects
directory_pipeline = os.path.join(main_directory, 'pipeline\\')

# directory to put students attached to building
directory_studentsbuildings = os.path.join(main_directory, 'studentsBuildings\\')

# directory for foreclosures
directory_foreclosures = os.path.join(main_directory, 'Foreclosed\\')

# directory for units counts
directory_units = os.path.join(main_directory, 'units\\')

# results directory
directory_results = os.path.join(main_directory, 'results\\Forecasts\\')