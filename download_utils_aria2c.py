import json, os, subprocess, time, itertools, datetime, base64
import traceback
from urllib.request import urlopen, build_opener, HTTPCookieProcessor, Request
from urllib.error import HTTPError
import urllib.request
from lxml import etree
from PIL import Image
# need aria2 install -> sudo apt install aria2
# need following 3rd lib -> 
# pip install aria2p[tui], future
# mamba install coda
import aria2p, coda

def write_log(*args):
    class bcolors:
        HEADER = '\033[95m'
        OKBLUE = '\033[94m'
        OKCYAN = '\033[96m'
        OKGREEN = '\033[92m'
        WARNING = '\033[93m'
        FAIL = '\033[91m'
        ENDC = '\033[0m'
        BOLD = '\033[1m'
        UNDERLINE = '\033[4m'
        
    if args[0] in ["ERROR","WARN","INFO","DEBUG"]:
        if args[0] == "ERROR":
            print(*([("{}[ERROR] {}".format(bcolors.FAIL,bcolors.ENDC))]+list(args)[1:]))
        elif args[0] == "WARN":
            print(*([("{}[WARN] {}".format(bcolors.WARNING,bcolors.ENDC))]+list(args)[1:]))
        else:
            print(*(["{}[{}] {}".format(bcolors.OKGREEN,args[0],bcolors.ENDC)]+list(args)[1:]))
    else:
        print(*args)
# if decorate request not None return request object
# else return encoded header 
def make_basic_header(username, password, decorate_request = None):
    charset = 'utf-8'
    auth_header_content = '{}:{}'.format(username,password)
    auth_header_content = base64.standard_b64encode(auth_header_content.encode(charset))
    auth_body = ("Authorization", "Basic %s" % auth_header_content.decode(charset))
    if decorate_request != None:
        decorate_request.add_header(*auth_body)
        return decorate_request
    return auth_body
# "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MOD021KM/2022/001.json"
# data_json = [obj1, obj2,...]
# obj_i = {"name":"filename", "size": [int], "last-modified": "YYYY-mm-dd HH:MM"}
def get_laads_json(LAADS_url_getlist_pattern, getlist_para, headers = []):
    if getattr(headers,"__iter__",None) == None: headers = [headers]
    while True:
        json_uri = LAADS_url_getlist_pattern.format(**getlist_para)
        try:
            opener = build_opener(HTTPCookieProcessor())
            request = urllib.request.Request(json_uri)
            for header in headers: request.add_header(*header)
            response = opener.open(request)
            data_json = json.loads(response.read())
            write_log("INFO", "downloaded json file", json_uri)
            break
        except:
            traceback.print_exc()            
            write_log("WARN", "get json file error", json_uri)
            time.sleep(5)
    return data_json

def get_e4ftl01_html(e4ftl01_url_getlist_pattern, getlist_para, headers = []):
    if getattr(headers,"__iter__",None) == None: headers = [headers]
    while True:
        html_uri = e4ftl01_url_getlist_pattern.format(**getlist_para)
        try:
            opener = build_opener(HTTPCookieProcessor())
            request = urllib.request.Request(html_uri)
            for header in headers: request.add_header(*header)
            response = opener.open(request)
            html_content = response.read()
            etree_element = etree.fromstring(html_content,parser=etree.HTMLParser(),base_url=html_uri)
            write_log("INFO", "downloaded html file", html_uri)
            break
        except HTTPError as http_err:
            # 404 and 403 mean unavailblity of the resource
            if http_err.code in [404,403]:
                write_log("INFO", "html request respond ({}):".format(http_err.code), html_uri)
                etree_element = etree.Element("root")
                break
        except:
            write_log("WARN", "get html file error", html_uri)
            time.sleep(5)
    return etree_element

def get_gesdisc_html(url_getlist_pattern, getlist_para, headers = []):
    if getattr(headers,"__iter__",None) == None: headers = [headers]
    while True:
        html_uri = url_getlist_pattern.format(**getlist_para)
        try:
            opener = build_opener(HTTPCookieProcessor())
            request = urllib.request.Request(html_uri)
            for header in headers: request.add_header(*header)
            response = opener.open(request)
            html_content = response.read()
            write_log("INFO", "downloaded html file", html_uri)
            etree_element = etree.fromstring(html_content,parser=etree.HTMLParser(),base_url=html_uri)
            break
        except HTTPError as http_err:
            # 404 and 403 mean unavailblity of the resource
            if http_err.code in [404,403]:
                write_log("INFO", "html request respond ({}):".format(http_err.code), html_uri)
                etree_element = etree.Element("root")
                break
        # assume server side error, wait until response
        except Exception as e:
            write_log("WARN", "get html file error", html_uri)
            time.sleep(5)
    return etree_element

def subfolders_from_etree(etree_root):
    return [hyper_link_ele.attrib["href"] \
        for hyper_link_ele in etree_root.xpath("//a[@href=text()]") \
        if ("http" not in hyper_link_ele.attrib["href"]) & \
        ("/" in hyper_link_ele.attrib["href"])]

def subfiles_from_etree(etree_root):
    return [hyper_link_ele.attrib["href"] \
        for hyper_link_ele in etree_root.xpath("//a[@href=text()]") \
        if ("http" not in hyper_link_ele.attrib["href"]) & \
        ("/" not in hyper_link_ele.attrib["href"])]

def get_subfolder_uri(base_uri, html_retrive_func, level = 1):
    uri_stacks = []
    def _recursion_body(base_uri, level):
        if level == 0: 
            uri_stacks.append(base_uri)
            return 
        else:
            etree_root = html_retrive_func(base_uri,{})
            folder_names = subfolders_from_etree(etree_root)
        for subfolders in folder_names:
            new_base_uri = base_uri + subfolders
            _recursion_body(new_base_uri,level=level-1)
    _recursion_body(base_uri, level)
    return uri_stacks
# class aria2c_Exception:
# not implemented
# class aria2c_manager:
#     def __init__(self):
#         _port_offset = 6801
#         self.worker_pool = []
#     def new(self):
#         pass

# for scientific data test
# .nc, .hdf, .xml, structured binary
def test_with_coda(file_path, use_subprocess=True):
    open_check = False
    if use_subprocess:
        return_code = subprocess.call(\
            ["python","-c",f"import coda; coda.open('{file_path}')"],\
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if return_code == 0:
            open_check = True
    else:
        try:
            with coda.open(file_path) as f: 
                open_check = True
        except:
            open_check = False
    return open_check

def test_with_PIP(file_path, use_subprocess=False):
    open_check = False
    if use_subprocess: 
        write_log("WARN", "Not implemented, use python kernel instead")
    try:
        with Image.open(file_path) as f: 
            open_check = True
    except:
        open_check = False
    return open_check

# add fuzzy match such as expectedsize == '20.1MB'
def test_with_size(file_path, expected_size):
    if os.path.isfile(file_path):
        if (str(os.path.getsize(file_path)) == str(expected_size)):
            return True
        else:
            return False
    else:
        return False
    
class aria2c_worker:
    # init -> add_task -> start -> check task_remain -> stop -> remove task
    def __init__(self, rpc_port, download_dir, \
        aria2_extra_conf = [], \
        aria2_parallel_conf = ["--max-connection-per-server=10", "-j20"], 
        initpause = True):
        #aria2_stdin = None):
        
        self.subprocess_obj = None
        self.rpc_port = rpc_port
        self.download_dir = download_dir
        self.tasks = []
        self.aria2_extra_conf = aria2_extra_conf
        self.aria2_parallel_conf = aria2_parallel_conf
        # self.aria2_stdin = aria2_stdin
        self._start_lock = False
        self.aria2_client = aria2p.Client(host="http://localhost", port=str(self.rpc_port),secret="")
        self.aria2c_args = ["aria2c", "--enable-rpc", "--rpc-listen-all", \
                        "--auto-file-renaming=false", "--allow-overwrite=true", \
                        "--rpc-listen-port="+str(self.rpc_port), \
                        "--dir="+str(self.download_dir)] \
                        + self.aria2_parallel_conf + self.aria2_extra_conf
        self.initpause = initpause
        if self.initpause:
            self.aria2c_args.append("--pause=true")
            self.subprocess_obj = subprocess.Popen(
                stderr = subprocess.DEVNULL, stdout = subprocess.DEVNULL, \
                args = self.aria2c_args)
    def dynamic_add_auto_start(self, uris, option = None, check_lock = True):
        if self.subprocess_obj == None:
            self.subprocess_obj = subprocess.Popen(
                    stderr = subprocess.DEVNULL, stdout = subprocess.DEVNULL, \
                    args = self.aria2c_args)
        aria2p_this_api = aria2p.API(self.aria2_client)
        for _ in range(10):
            # wait until aria2c startup is ready
            try:
                time.sleep(1)
                _ = aria2p_this_api.get_stats()
                break
            except:
                time.sleep(2)
                write_log("INFO", "aria2c daemon is not ready")
        self.aria2_client.add_uri(uris if isinstance(uris,list) else [uris], option)
        if self.initpause:
            self.aria2_client.unpause_all()
        # for task_idx,task in enumerate(self.tasks):
        #     _gid = self.aria2_client.add_uri(task["uri"] if isinstance(task["uri"],list) else [task["uri"]] , task["option"])
        #     self.tasks[task_idx]["gid"] = _gid

    def start(self,):
        # if self._start_lock == True:
        #     return

        self.stop()
        # while self.subprocess_obj != None:
        #     while self.subprocess_obj.poll() == None:
        #         if self.subprocess_obj == None: break
        #         else: time.sleep(1)

        self._start_lock = True
        if len(self.tasks) > 0:
            self.subprocess_obj = subprocess.Popen(
                stderr = subprocess.DEVNULL, stdout = subprocess.DEVNULL, \
                args = self.aria2c_args)
            # print(" ".join(["aria2c", "--enable-rpc", "--rpc-listen-all", \
            #         "--auto-file-renaming=false", "--allow-overwrite=true", \
            #         "--rpc-listen-port="+str(self.rpc_port), \
            #         "--dir="+str(self.download_dir)]))
            aria2p_this_api = aria2p.API(self.aria2_client)
            for _ in range(10):
                # wait until aria2c startup is ready
                try:
                    time.sleep(1)
                    _ = aria2p_this_api.get_stats()
                    break
                except:
                    time.sleep(2)
                    write_log("INFO", "aria2c daemon is not ready")
            for task_idx,task in enumerate(self.tasks):
                _gid = self.aria2_client.add_uri(task["uri"] if isinstance(task["uri"],list) else [task["uri"]] , task["option"])
                self.tasks[task_idx]["gid"] = _gid
                

    def stop(self):
        if isinstance(self.subprocess_obj, subprocess.Popen):
            self.subprocess_obj.terminate()
            while True:
                time.sleep(1)
                try:
                    if self.subprocess_obj.poll() != None:
                        break
                except:
                    break
        del self.subprocess_obj
        self.subprocess_obj = None
        self._start_lock = False
            
    def add_task(self, uris, option = None, check_lock = True):
        if check_lock & self._start_lock: raise Exception; return
        # aria2c_gid, url, option, retried times  start from 0
        self.tasks.append({"gid":"", "uri":uris, "option":option, "status":"unknown"})
        # remove duplicated tasks
    
    def task_remain(self):
        # write_log("DEBUG","start scan task")
        aria2p_this_api = aria2p.API(self.aria2_client)
        dl_infos = aria2p_this_api.get_downloads()
        status_mapping = {dl_info.gid:dl_info.status for dl_info in dl_infos}
        remain_count = 0
        for task_idx,task in enumerate(self.tasks):
            aria2c_status4gid = status_mapping[task["gid"]]
            if task["status"] in ["unknown", "active"]:
                if aria2c_status4gid == "complete":
                    write_log("INFO", "job complete:", task["uri"])
                elif aria2c_status4gid == "error":
                    write_log("ERROR", "job failed:", task["uri"])
                else:
                    # active or waiting status
                    remain_count += 1
            self.tasks[task_idx]["status"] = aria2c_status4gid
        # write_log("DEBUG","end scan task")    
        return remain_count
    def remove_tasks(self, check_lock = True):
        if check_lock & self._start_lock: raise Exception; return
        self.tasks = []
    # def remove_tasks(self, complete_only = True):
    #     if self._start_lock: raise Exception; return
    #     if complete_only:
    #         self.tasks = []
        