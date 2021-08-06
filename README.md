# VAERS  


<!--TOC-->
* [VAERS](#vaers)
	* [Disclaimer](#disclaimer)
	* [get started](#get-started)
		* [1. download the data from VAERS website](#1.-download-the-data-from-vaers-website)
		* [2. move the AllVAERSDataCSVS.zip to VAERS folder and unzip it](#2.-move-the-allvaersdatacsvs.zip-to-vaers-folder-and-unzip-it)
		* [3. check the files](#3.-check-the-files)
		* [4. run main() from VAERS.py](#4.-run-main()-from-vaers.py)

<!--TOC-->
## Disclaimer
**THIS IS NOT MEDICAL ADVICE**  
if you want medical advice please go see your doctor.

please also read the disclaimer on the VAERS data on their website. 
[VAERS website](https://vaers.hhs.gov/data.html)

## get started
### 1. download the data from VAERS website

[link to VAERS website](https://vaers.hhs.gov/data/datasets.html?)

this will give you AllVAERSDataCSVS.zip

### 2. move the AllVAERSDataCSVS.zip to VAERS folder and unzip it

### 3. check the files

you should have at least 6 files   
```
.../VAERS/AllVAERSDataCSVS/  
    2021VAERSVAX.csv  
    2021VAERSSYMPTOMS.csv  
    2021VAERSDATA.csv  
    2020VAERSVAX.csv  
    2020VAERSSYMPTOMS.csv  
    2020VAERSDATA.csv  
```

### 4. run main() from VAERS.py

this will merge the files above (if it's the first time running).
and print out some stats about the data.  

