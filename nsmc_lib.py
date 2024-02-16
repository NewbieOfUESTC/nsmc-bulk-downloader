import requests
import json
import re
import time
import datetime
import base64
import hashlib
import os
import io
import imageio
import easyocr
import socket, errno


nsmc_file_fmt = "./ftp.nsmc.org.cn/{product_name}/{year}/{month:0>2}/{day:0>2}/{fname}"
output_log = True
use_aria = True

if use_aria == False:
    from ftplib import FTP
    from concurrent.futures import ProcessPoolExecutor
from download_utils_aria2c import *


def is_port_used(port, ip="127.0.0.1"):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    is_used = True
    try:
        s.bind((ip, port))
        is_used = False
    except socket.error as e:
        is_used = True
        if e.errno == errno.EADDRINUSE:
            print(f"Port: {port} is already in use")
        else:
            # something else raised the socket.error exception
            print(e)
    s.close()
    return is_used
ARIA_PORT = 0
for port_select in range(6801,7000):
    if is_port_used(port_select):
        continue
    else:
        GLOBAL_aria_worker = aria2c_worker(\
            rpc_port = port_select,\
            download_dir = "/tmp",\
            aria2_extra_conf = ["--continue=false"],\
            aria2_parallel_conf=\
                ["--max-connection-per-server=12", \
                 "-j12",\
                 "--max-tries=5",\
                 "--retry-wait=10"],\
            initpause=True)
        ARIA_PORT = port_select
        write_log("INFO","init Aria at port", ARIA_PORT)
        break

def file_examer(file_path, external_info={}):
    #coda check
    if test_with_coda(file_path):
        # print("open okay")
        if "DATASIZE" in external_info.keys():
            # size check
            if test_with_size(file_path, external_info["DATASIZE"]):
                # print("size okay")
                return True
            else:
                return False
        else:
            return True
    # print("not okay")
    # print(file_path)
    return False

def download_using_ftp(url,target_format=nsmc_file_fmt):
    # Parse the URL
    user, credentials, server, path = re.search(r"ftp:\/\/(.*?):(.*?)@(.*?)(\/.*)", url).groups()

    # Connect to the FTP server
    ftp = FTP(server)
    ftp.login(user=user, passwd=credentials)
    fname = path.split("/")[-1]
    product_name = re.split("_[0-9]{8}_",fname)[0]
    product_date = re.findall("[0-9]{8}",fname)[0]
    product_year, product_month, product_day = product_date[:4], product_date[4:6], product_date[6:8]
    new_fdir = target_format.format(\
        product_name=product_name,\
        year=product_year, \
        month=product_month, \
        day=product_day, \
        fname="")
    # os.path.join(new_base_dir,product_name,product_year, product_month, product_day)
    new_fpath = os.path.join(new_fdir, fname)
    os.makedirs(new_fdir,exist_ok=True)
    # Download the file
    with open(new_fpath, "wb") as f:
        ftp.retrbinary(f"RETR {path}", f.write)
    print("Downloaded",new_fpath)
    ftp.quit()
    
def download_using_aria(url,target_format=nsmc_file_fmt,worker=GLOBAL_aria_worker):
    # Parse the URL
    user, credentials, server, path = re.search(r"ftp:\/\/(.*?):(.*?)@(.*?)(\/.*)", url).groups()
    # prepare download dirctory
    fname = path.split("/")[-1]
    product_name = re.split("_[0-9]{8}_",fname)[0]
    product_date = re.findall("[0-9]{8}",fname)[0]
    product_year, product_month, product_day = product_date[:4], product_date[4:6], product_date[6:8]
    new_fdir = target_format.format(\
        product_name=product_name,\
        year=product_year, \
        month=product_month, \
        day=product_day, \
        fname="")
    # os.path.join(new_base_dir,product_name,product_year, product_month, product_day)
    new_fpath = os.path.join(new_fdir, fname)
    os.makedirs(new_fdir,exist_ok=True)
    # worker.add_task(url, {"dir":new_fdir}, check_lock=False)
    worker.dynamic_add_auto_start(url, {"dir":new_fdir})
    return worker
    
def query_all(condition={}):
    QUERY_INTERVAL = 50
    QUERY_MAX_PAGE = 1000000
    query_stack = []
    for beginindex in range(1,1000000,QUERY_INTERVAL):
        url = 'http://satellite.nsmc.org.cn/PortalSite/WebServ/DataService.asmx/GetArcDatasByProduction'

        headers = {
            'Content-Type': 'application/json;charset=utf-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'satellite.nsmc.org.cn',
            'Origin': 'http://satellite.nsmc.org.cn',
            'Connection': 'keep-alive',
            'Referer': 'http://satellite.nsmc.org.cn/PortalSite/Data/FileShow.aspx',
            'X-Requested-With': 'XMLHttpRequest'
        }
        # converStatus == "Part" : intersection
        # converStatus == "All"  : area within satellite extent
        data = {
            'productID': 'FY3E_MERSI_GRAN_L1_YYYYMMDD_HHmm_1000M_Vn.HDF,FY3E_MERSI_GRAN_L1_YYYYMMDD_HHmm_GEO1K_Vn.HDF',
            'txtBeginDate': '2022-05-01',
            'txtBeginTime': '00:00:00',
            'txtEndDate': '2022-05-01',
            'txtEndTime': '23:59:59',
            'East_CoordValue': '180',
            'West_CoordValue': '-180',
            'North_CoordValue': '90',
            'South_CoordValue': '45',
            'converStatus': 'Part',
            'cbAllArea': '',
            'rdbIsEvery': 'on',
            'beginindex': beginindex,
            'endindex': beginindex+QUERY_INTERVAL-1,
            'sortName': 'DATABEGINDATE',
            'sortOrder': 'desc',
            'where': ''
        }
        data.update(condition)
        response_code = 500

        while response_code != 200:
            try:
                response = requests.post(url, headers=headers, data=json.dumps(data), )
                response_code = response.status_code
                if response_code == 200:
                    # print("Request successful!")
                    invalid_json_string = response.json()["d"]
                else:
                    # print("Request failed with status code:", response.status_code)
                    time.sleep(1)
            except:
                time.sleep(10)
        if invalid_json_string == '':
            break
        # This JSON string has properties without any quotes, which is not valid JSON
        invalid_json_string = invalid_json_string.replace("'","\"")

        # Define a regular expression pattern to match unquoted property names
        property_name_pattern1 = re.compile(r',(\w+):')
        property_name_pattern2= re.compile(r'{(\w+):')

        # Replace unquoted property names with double-quoted property names
        valid_json_string = property_name_pattern1.sub(r',"\1":', invalid_json_string)
        valid_json_string = property_name_pattern2.sub(r'{"\1":', valid_json_string)
        jobj = json.loads(valid_json_string)
        query_stack.append(jobj)
    query_stack = sum(query_stack,[])
    return query_stack

def query_cart(condition={},cookies={}):
    QUERY_INTERVAL = 50
    QUERY_MAX_PAGE = 1000000
    query_stack = []
    for beginindex in range(1,1000000,QUERY_INTERVAL):
        url = 'http://satellite.nsmc.org.cn/PortalSite/WebServ/CommonService.asmx/GetShoppingCart'

        headers = {
            'Content-Type': 'application/json;charset=utf-8',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Host': 'satellite.nsmc.org.cn',
            'Origin': 'http://satellite.nsmc.org.cn',
            'Connection': 'keep-alive',
            'Referer': 'http://satellite.nsmc.org.cn/PortalSite/Data/FileShow.aspx',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        data = {
            'where':'',
            'sortName':'DATABEGINDATE',
            'sortOrder':'desc',
            'beginindex': beginindex,
            'endindex': beginindex+QUERY_INTERVAL-1,
        }
        data.update(condition)
        response_code = 500
        while response_code != 200:
            try:
                response = requests.post(url, headers=headers, data=json.dumps(data),cookies=cookies)
                response_code = response.status_code
                if response_code == 200:
                    # print("Request successful!")
                    invalid_json_string = response.json()["d"]
                else:
                    # print("Request failed with status code:", response.status_code)
                    time.sleep(2)
            except:
                time.sleep(100)
        if (invalid_json_string == '') | (invalid_json_string == '[]') | (invalid_json_string == '{}'):
            break
        # This JSON string has properties without any quotes, which is not valid JSON
        invalid_json_string = invalid_json_string.replace("'","\"")

        # Define a regular expression pattern to match unquoted property names
        property_name_pattern1 = re.compile(r',(\w+):')
        property_name_pattern2= re.compile(r'{(\w+):')

        # Replace unquoted property names with double-quoted property names
        valid_json_string = property_name_pattern1.sub(r',"\1":', invalid_json_string)
        valid_json_string = property_name_pattern2.sub(r'{"\1":', valid_json_string)
        jobj = json.loads(valid_json_string)
        query_stack.append(jobj)
    query_stack = sum(query_stack,[])
    return query_stack

def query_cartinfo(condition={},cookies={}):

    query_stack = []
    url = 'http://satellite.nsmc.org.cn/portalsite/WebServ/CommonService.asmx/BindShowCartInfo'

    headers = {
        'Content-Type': 'application/json;charset=utf-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'satellite.nsmc.org.cn',
        'Origin': 'http://satellite.nsmc.org.cn',
        'Connection': 'keep-alive',
        'Referer': 'http://satellite.nsmc.org.cn/PortalSite/Data/FileShow.aspx',
        'X-Requested-With': 'XMLHttpRequest'
    }

    response_code = 500
    while response_code != 200:
        try:
            response = requests.post(url, headers=headers, cookies=cookies)
            response_code = response.status_code
            if response_code == 200:
                # print("Request successful!")
                invalid_json_string = response.json()["d"]
            else:
                # print("Request failed with status code:", response.status_code)
                time.sleep(1)
        except:
            time.sleep(10)
    return (invalid_json_string)

def add_one_to_cart(condition={},cookies={}):
    query_stack = []
    url = 'http://satellite.nsmc.org.cn/PortalSite/WebServ/CommonService.asmx/selectOne'

    headers = {
        'Content-Type': 'application/json;charset=utf-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'satellite.nsmc.org.cn',
        'Origin': 'http://satellite.nsmc.org.cn',
        'Connection': 'keep-alive',
        'Referer': 'http://satellite.nsmc.org.cn/PortalSite/Data/FileShow.aspx',
        'X-Requested-With': 'XMLHttpRequest'
    }


    data = {
        'filename':'FY3E_MERSI_GRAN_L1_20230225_2000_1000M_V0.HDF', \
        'satellitecode':'FY3E', \
        'datalevel':'L1',\
        'ischecked':'true'
    }
    data.update(condition)
    response_code = 500
    while response_code != 200:
        try:
            response = requests.post(url, data=json.dumps(data), headers=headers, cookies=cookies)
            response_code = response.status_code
            if response_code == 200:
                # print("Request successful!")
                invalid_json_string = response.json()["d"]
            else:
                # print("Request failed with status code:", response.status_code)
                time.sleep(1)
        except:
            time.sleep(10)
    return (invalid_json_string)

def clear_cart(cookies={}):

    query_stack = []
    url = 'http://satellite.nsmc.org.cn/portalsite/WebServ/CommonService.asmx/DeleteByCart'

    headers = {
        'Content-Type': 'application/json;charset=utf-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'satellite.nsmc.org.cn',
        'Origin': 'http://satellite.nsmc.org.cn',
        'Connection': 'keep-alive',
        'Referer': 'http://satellite.nsmc.org.cn/PortalSite/Data/FileShow.aspx',
        'X-Requested-With': 'XMLHttpRequest'
    }

    data = {'type':'1','where':'','selectitem':'','selectall':'1'}
    
    response_code = 500
    while response_code != 200:
        try:
            response = requests.post(url, data=json.dumps(data), headers=headers, cookies=cookies)
            response_code = response.status_code
            if response_code == 200:
                # print("Request successful!")
                invalid_json_string = response.json()["d"]
            else:
                # print("Request failed with status code:", response.status_code)
                time.sleep(1)
        except:
            time.sleep(10)
    return (invalid_json_string)

def submit_cart(condition={},cookies={}):

    query_stack = []
    url = 'http://satellite.nsmc.org.cn/PortalSite/WebServ/CommonService.asmx/Submit'

    headers = {
        'Content-Type': 'application/json;charset=utf-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'satellite.nsmc.org.cn',
        'Origin': 'http://satellite.nsmc.org.cn',
        'Connection': 'keep-alive',
        'Referer': 'http://satellite.nsmc.org.cn/PortalSite/Data/FileShow.aspx',
        'X-Requested-With': 'XMLHttpRequest'
    }

    data = {'chkIsPushMode':'false','chkIsSendMail':'false','radioBtnlist_ftp':'0'}
    
    data.update(condition)
    response_code = 500
    while response_code != 200:
        try:
            response = requests.post(url, data=json.dumps(data), headers=headers, cookies=cookies)
            response_code = response.status_code
            if response_code == 200:
                # print("Request successful!")
                invalid_json_string = response.json()["d"]
            else:
                # print("Request failed with status code:", response.status_code)
                time.sleep(1)
        except:
            time.sleep(10)
    return (invalid_json_string)
    # 'A202303250493317311@1@67304334@2023/3/25'
    # '购物车中没有商品'
    # http://file.nsmc.org.cn/ORDERFILELIST/A202303250493317311.txt
    
def query_limitation(cookies={}):

    query_stack = []
    url = 'http://satellite.nsmc.org.cn/PortalSite/Data/ShoppingCart.aspx'

    headers = {
        'Content-Type': 'application/json;charset=utf-8',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'satellite.nsmc.org.cn',
        'Origin': 'http://satellite.nsmc.org.cn',
        'Connection': 'keep-alive',
        'Referer': 'http://satellite.nsmc.org.cn/PortalSite/Data/FileShow.aspx',
        'X-Requested-With': 'XMLHttpRequest'
    }
    
    response_code = 500
    while response_code != 200:
        try:
            response = requests.post(url, headers=headers, cookies=cookies)
            response_code = response.status_code
            if response_code == 200:
                # print("Request successful!")
                invalid_json_string = response.text
            else:
                # print("Request failed with status code:", response.status_code)
                time.sleep(1)
        except:
            time.sleep(10)
    html_content = invalid_json_string
        
    # Regex pattern to match the element with the specific ID
    pattern = re.compile(r'<[^>]*id="lblDayFree"[^>]*>(.*?)<\/[^>]+>', re.IGNORECASE)
    match = pattern.search(html_content)
    text_content = match.group(1)
    number_part = float(re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", text_content)[0])
    if ("GB" in text_content) | ("gB" in text_content):
        number_part = number_part*(1e9)    
    if ("MB" in text_content) | ("mB" in text_content):
        number_part = number_part*(1e6)
    if ("KB" in text_content) | ("kB" in text_content):
        number_part = number_part*(1e3)

    return (number_part)

def login(cookies={}, data = None):

    query_stack = []
    url = 'http://satellite.nsmc.org.cn/PortalSite/Sup/user/Login.aspx'

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'satellite.nsmc.org.cn',
        'Origin': 'http://satellite.nsmc.org.cn',
        'Connection': 'keep-alive',
        'Referer': 'http://satellite.nsmc.org.cn/PortalSite/Sup/user/Login.aspx',
    }
    
    response_code = 500
    while response_code != 200:
        try:
            response = requests.post(url, headers=headers, cookies = cookies, data=data, timeout=60)
            response_code = response.status_code
            if response_code == 200:
                # print("Request successful!")
                invalid_json_string = response.text
            else:
                # print("Request failed with status code:", response.status_code)
                time.sleep(1)
        except:
            time.sleep(10)
    return (response)

def get_verf_code(date_timestamp, cookies={}):

    query_stack = []
    url = 'http://satellite.nsmc.org.cn/PortalSite/Sup/User/LoginGenCodeImg.aspx'

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'satellite.nsmc.org.cn',
        'Connection': 'keep-alive',
        'Referer': 'http://satellite.nsmc.org.cn/PortalSite/Sup/User/LoginGenCodeImg.aspx',
    }
    
    response_code = 500
    while response_code != 200:
        # ?date=3/25/2023%204:06:42%20PM
        response = requests.get(url, headers=headers, cookies=cookies, params={"date":date_timestamp})
        response_code = response.status_code
        if response_code == 200:
            # print("Request successful!")
            invalid_json_string = response.text
        else:
            # print("Request failed with status code:", response.status_code)
            time.sleep(1)
    return (response)

def ocr_image(image_bin, ):
    output = io.BytesIO()
    imageio.imwrite(output,imageio.imread(image_bin, format="gif"),"png")
    reader = easyocr.Reader(['en'], gpu=False)
    # print(type(output.getvalue()))
    # print(reader.readtext(output.getvalue()))
    result = reader.readtext(output.getvalue())[0][1]
    result = re.sub(r'[^\w]', '', result)
    #print(base64.b64encode(image_bin).decode('utf-8'))
    # print(result)
    time.sleep(1)
    return result

def get_login_token(userid, userpwd, ):
    while True:
        resp = login(cookies={"UserLanguage":"en-US"})
        cookies_before_login = resp.cookies.get_dict()
        # Original date string
        date_str = resp.headers['Date']

        # Parse the date string into a datetime object
        date_obj = datetime.datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %Z')

        # Reformat the datetime object into the desired format
        formatted_date_str = date_obj.strftime('%-m/%-d/%Y %I:%M:%S %p')

        # print(formatted_date_str)  # Output: '3/25/2023 04:25:40 PM'
        resp_img = get_verf_code(formatted_date_str, cookies_before_login)
        result = ocr_image(resp_img.content, )
        write_log("INFO",f"ocr verf code: {result}")
        # password to MD5
        md5_hash_pwd = hashlib.md5(userpwd.encode('utf-8')).hexdigest()
        cookies_before_login.update(resp_img.cookies.get_dict())
        login_data = {\
            "__EVENTTARGET":"",\
            "__EVENTARGUMENT":"",\
            "TextBox_UserID":userid,\
            "TextBox_Psw":md5_hash_pwd,\
            "TextBox_Code":result,\
            "btnOk.x": 37,\
            "btnOk.y": 20,\
            }
        for _id_cookie in ["__VIEWSTATE", "__VIEWSTATEGENERATOR","__EVENTVALIDATION"]:
            pattern = r'(<[^<>]+id="{}"[^<>]*>)'.format(_id_cookie)
            match = re.search(pattern,resp.text.replace("\n","").replace("\r",""))
            text_content = match.group(1)
            _value = re.findall(r'<[^<>]+value="(.*?)"[^<>]*>', text_content)[0]
            login_data.update({_id_cookie:_value})
        resp2 = login(cookies_before_login, login_data)
        login_info = resp2.cookies.get_dict()
        if ".ASPXAUTH" in login_info.keys():
            break
        else:
            time.sleep(1)
    return login_info

def download_task(task, login_info, target_format=nsmc_file_fmt):
    issue_stack = []
    sum_size = 0
    QUOTA_THRESHOLD = 1_000_000_000
    reached_quota = False
    # query file 
    target_file_info = query_all(task)
    write_log("INFO","query done")

    # wait until quota is enough
    while True:
        # login
        login_info_cookies = get_login_token(**login_info)
        write_log("INFO","login okay: ",login_info_cookies)
        # query quota
        maxquota_today = query_limitation(login_info_cookies)
        if maxquota_today < (QUOTA_THRESHOLD*2):
            # wait 1 hour for quota update
            time.sleep(60*60)
            write_log("INFO","reach max quota today, wait 1 hour")
            # write_log("INFO", "job complete:", task["uri"])
        else:
            break
    write_log("INFO","query quota:", maxquota_today)
    # clear the cart first
    clear_cart(login_info_cookies)
    write_log("INFO","cart cleared!")
    # filter local file
    for one_target_file_info in target_file_info:
        fname = one_target_file_info["ARCHIVENAME"]
        product_name = re.split("_[0-9]{8}_",fname)[0]
        product_date = re.findall("[0-9]{8}",fname)[0]
        product_year, product_month, product_day = \
            product_date[:4], product_date[4:6], product_date[6:8]
        file_full_path = target_format.format(\
            product_name=product_name,\
            year=product_year, \
            month=product_month, \
            day=product_day, \
            fname=fname)
        if file_examer(file_full_path, one_target_file_info):
            pass
        else:
            fsize = int(one_target_file_info["DATASIZE"])
            # left 1G quota
            if maxquota_today > (fsize + sum_size + QUOTA_THRESHOLD):
                one_target_file_info.update({"dl_destination":file_full_path})
                issue_stack.append(one_target_file_info)
                sum_size += int(one_target_file_info["DATASIZE"])
            else:
                reached_quota = True
                write_log("INFO","Reach daily quota, iterate in next loop")
                break
            # remove incomplete files
            if os.path.exists(file_full_path):
                os.remove(file_full_path)
            if os.path.exists(file_full_path+".aria2"):
                os.remove(file_full_path+".aria2")
            
    write_log("INFO","file download scheduler:")
    for issue_product in issue_stack:
        fname = one_target_file_info["ARCHIVENAME"]
        product_name = re.split("_[0-9]{8}_",fname)[0]
        product_date = re.findall("[0-9]{8}",fname)[0]
        add_one_to_cart({\
            "filename":issue_product["ARCHIVENAME"],\
            'satellitecode':product_name.split("_")[0], \
            'datalevel':product_name.split("_")[-1],\
            },\
            login_info_cookies)
        write_log("INFO",issue_product["ARCHIVENAME"])
    if len(issue_stack) == 0:
        return 
    # for loop
    # add these files to cart
    # check cart with choosen list
    # end loop <- if match
    # sumbit cart
    
    order_ids = submit_cart(cookies=login_info_cookies)
    write_log("INFO","order submitted",order_ids)
    order_ids = order_ids.split("|")[-1].split("&")
    order_ids_dict = {order_id: False for order_id in order_ids}
    results = None
    # hang until all orders finished
    while True:
        for order_id in order_ids_dict.keys():
            try:
                resp = requests.get("http://file.nsmc.org.cn/ORDERFILELIST/{}.txt"\
                    .format(order_id.split("@")[0]))
                # check by ten minutes until return 200
                if (resp.status_code == 200) & (order_ids_dict[order_id] == False): 
                    order_ids_dict[order_id] = True
                    # add download task
                    if use_aria:
                        results = None
                        for line in resp.text.splitlines():
                            results = download_using_aria(line,)
                        # if worker != None:
                        #     worker.start()
                        write_log("INFO",f"task added to aria. port: {ARIA_PORT}", order_id)
                        # return results
                    # download one by one
                    else:
                        write_log("INFO","start downloading using FTPLIB", order_id)
                        max_concurrent_count = 5
                        with ProcessPoolExecutor(max_workers=max_concurrent_count) as executor:
                            results = executor.map(download_using_ftp, \
                                [line for line in resp.text.splitlines()])
                        # for result in results:
                        #     print(result)
                        write_log("INFO","task downloaded using FTPLIB", order_id)
                        # return results
            except: 
                time.sleep(60*1)
        if all(order_ids_dict.values()):
            break
        else:
            time.sleep(60*5)
    if reached_quota:
        time.sleep(60*60)
        results = download_task(task, login_info, target_format)
    return results
