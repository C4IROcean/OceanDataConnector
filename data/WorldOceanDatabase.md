# World Ocean Database

## Overview
The data available through the Python SDK is gathered from the World Ocean Database. The World Ocean Database which is a National Centers for Environmental (NCEI) product and International Oceanographic Data and Information Exchange (IODE) project which provides a composite of publicly available ocean profile data, both historic and recent. It consists of over thousands of datasets consisting of millions of water temperatures, salinity, oxygen, and nutrient profiles (1,2).

### Data Organisation and Definitions

Cast: A set of one or more profiles taken concurrently or nearly concurrently. All casts from similar instruments with similar resolutions are grouped together. For example, all bathythermograph (BT) data are all part of the same data set (MBT), see below.

Profile: A set of measurements for a single variable (i.e. temperature salinity) along a specific path, which could be vertically in the water column, horizontally along the surface, or discrete areas based on placement of buoys.

### Data Description

The data available through the Ocean Data Platform are oceanographic measurements of physical and chemical ocean parameters (temperature, salinity, oxygen, nitrate, ph and chlorophyll), and is based on the data available through NOAA's World Ocean Database. Each cast has a sepcified latitude, longitude and time (lat, lon and datetime), and a depth profile, where each depth has measured physical and chemical parameters. Not all casts have all the the different ocean parameters, missing measurements are populated with nans. 

Each measurements has a WODflag parameter (i.e Nitrate_WODflag). If flag value is zero, there are no known issues with the measured value.

For more information on the types of flags, see:

https://www.nodc.noaa.gov/OC5/WOD/CODES/Definition_of_Quality_Flags.html

### Datasets

Dataset code | Dataset includes
--- | --- 
OSD| Ocean Station Data, Low-resolution CTD/XCTD, Plankton data
CTD| High-resolution Conductivity-Temperature-Depth / XCTD data
MBT| Mechanical / Digital / Micro Bathythermograph data
XBT| Expendable Bathythermograph data
SUR| Surface-only data
APB| Autonomous Pinniped data
MRB| Moored buoy data
PFL| Profiling float data
DRB| Drifting buoy data
UOR| Undulating Oceanographic Recorder data
GLD| Glider data

### References:
1.	Boyer, T.P., O.K. Baranova, C. Coleman, H. E., Garcia, A. Grodsky, R.A. Locarnini, A. V., Mishonov, C.R. Paver, J.R. Reagan, D. S. & I.V. Smolyar, K.W. Weathers,  and M. M. Z. World Ocean Atlas 2018, Volume 1: Temperature. Tech. Ed. NOAA Atlas NESDIS 87 (2018).
2.	Boyer, T. P. et al. World Ocean Database 2018. NOAA Atlas NESDIS 87 (2018).

