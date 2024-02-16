from nsmc_lib import *

userid = ""
userpwd = ""

if __name__ == "__main__":
    # while True:
    _INTERVAL = 1
    for i in range(0,700,_INTERVAL):
        this_date = datetime.datetime.strftime(\
            datetime.datetime(2022,5,14) + \
            datetime.timedelta(days=i),"%Y-%m-%d")
        next_date = datetime.datetime.strftime(\
            datetime.datetime(2022,5,14) + \
            datetime.timedelta(days=i+_INTERVAL-1),"%Y-%m-%d")
        print(this_date,next_date)
        try:

            this_worker = download_task({
                'productID': "FY3E_MERSI_GRAN_L1_YYYYMMDD_HHmm_1000M_Vn.HDF,"\
                                "FY3E_MERSI_GRAN_L1_YYYYMMDD_HHmm_GEO1K_Vn.HDF",
                'txtBeginDate': this_date,
                'txtBeginTime': '00:00:00',
                'txtEndDate': next_date,
                'txtEndTime': '23:59:59',
                'East_CoordValue': '150',
                'West_CoordValue': '125',
                'North_CoordValue': '-10',
                'South_CoordValue': '-25',},\

                dict(userid = userid, \
                userpwd = userpwd, ))
        except Exception as e:
            print(this_date, e)
    while this_worker.task_remain() > 0:
        time.sleep(10*60)
