import geopandas as gpd

stations = gpd.read_file('/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_acc_15s.tif')
stations2 = gpd.read_file('/home/sar_hydro/STUDIES/EtudesEB/HydroSHED/other/hyd_eu_dir_15s.tif')

print(stations.head())
print(stations.crs)
print(stations2.head())
print(stations2.crs)