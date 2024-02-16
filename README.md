# What can this code do?
It provides a pipeline for retrieving satellite data from NSMC, especially, for downloading bulk of data as a continous task. It supports for automatically login, preparing order, submitting order and processing the download in the order. (Attention!!) The data in the cart will be removed during the processing, make sure your account is only used for this software only.
# How to run?
python ./fy_download.py  
# What need to do before running your code?
## Install necessay librarys  
- easyocr
- lxml
- aria2p
- coda
## Optinal software to install  
- aria2
## Edit the necessay variables
### fy_download.py  
userid, userpwd, this_date, next_date, and the parameters in the function download_task
### nsmc_lib.py
nsmc_file_fmt
