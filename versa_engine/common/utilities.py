import glob
import os
import shutil
import time
import socket
import subprocess
import sys
import importlib
import collections
import getpass
import tempfile
from multiprocessing.connection import Listener
from multiprocessing.connection import Client
from urllib.parse import urlparse
import requests
from hashlib import sha256

from versa_engine.common import xmlutils as xu
import importlib.util
module_dir = os.path.dirname(os.path.realpath(__file__))


def get_username():
    user = getpass.getuser()
    return user


def get_last_file_by_pattern(pattern=None, wdir='./'):
    if not wdir.endswith('/'):
        wdir = wdir + '/'

    files = glob.glob(wdir + pattern)
    if not files:
        return None
    if files:
        files.sort(key=os.path.getmtime)
        return files[-1][len(wdir):]


def import_module(module_dir="./", module_name=None):

    spec = importlib.util.spec_from_file_location(
        module_name, f'{module_dir}/{module_name}.py')
    foo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(foo)
    return foo


def get_attr_value_from_module(module_dir, module_name, module_attr):
    module_obj = import_module(module_dir, module_name)
    return getattr(module_obj, module_attr)


def remove_dir(dir):
    try:
        shutil.rmtree(dir)
    except:
        sys.stderr.write("remove_dir: dir not found " + dir + "\n")
        return


def gethostname():
    return socket.gethostname()


def timing(f):
    def wrap(*args):
        time1 = time.time()
        ret = f(*args)
        time2 = time.time()
        print('%s (%r) function took %0.3f ms' %
              (f.func_name,  args, (time2-time1)*1000.0))
        return ret
    return wrap


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print('%r (%r, %r) %2.2f sec' %
              (method.__name__, args, kw, te-ts))
        return result
    return timed


def build_work_dir():
    work_dir = tempfile.mkdtemp()
    return work_dir
#     p = subprocess.Popen('mktemp -d', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#     work_dir=p.stdout.readline().rstrip()
#     return work_dir


def get_directory(path):
    return os.path.dirname(path)


def get_filename(path):
    return os.path.basename(path)


def wait_for_file(fn, msg, wait_time=5):
    while True:
        if not os.path.isfile(fn):
            print(msg)
            time.sleep(wait_time)
        else:
            return


def get_listener(port):
    print("get listener = ", gethostname())
    address = (gethostname(), port)     # family is deduced to be 'AF_INET'
    listener = Listener(address, authkey=b'secret password')
    return listener

    print("get listener = ", gethostname())


def signal_listen(listener, num_senders=1):
    assert type(listener) is not int  # previous incarnation of used port
    all_msgs = []
    for i in range(0, num_senders):
        conn = listener.accept()
        msg = conn.recv()
        all_msgs.append(msg)
    return all_msgs


def signal_send(host, port, msg='database loaded'):
    print("signal_send ", host)
    address = (host, port)     # family is deduced to be 'AF_INET'
    conn = Client(address, authkey=b'secret password')
    conn.send(msg)
    conn.close()


def count_iterable(i):
    return sum(1 for e in i)


#system_config_xml_fn = None
#import os.path
#from  system_config import config_dir

#    system_config_xml_fn = module_dir + '/haswell_system_config.xml'
# if os.path.isfile(config_dir+ '/dicex_shadowfax.sh'):
#    system_config_xml_fn = module_dir + '/shadowfax_system_config.xml'


# TODO: how does system_config.xml gets build on new system
system_config_root = xu.read_file("/home/kabira/.versa/" + 'system_config.xml')
#system_config_root = xu.read_file(config_dir + '/system_config.xml')

#cluster_login_ip = xu.get_value_of_key(system_config_root, 'port_server_ip')


def get_system_config_root():
    return system_config_root


def get_new_port(port_server_ip=None):
    global cluster_login_ip
    if port_server_ip is None:
        port_server_ip = cluster_login_ip
    s = socket.socket()
    rport = 39785
    s.connect((port_server_ip, rport))
    port = s.recv(1024).decode()
    s.close()
    return int(port)


# def get_port_server_ip(system_config_root):
#    port_server_ip=xu.get_value_of_key(system_config_root, 'port_server_ip')
#    return port_server_ip

# def get_cluster_name(system_config_root):
#    cluster_name = xu.get_value_of_key(system_config_root, 'cluster_name')
#    return cluster_name

def get_epifast_exec_path(system_config_root):
    exec_path = xu.get_value_of_key(system_config_root, 'epifast_exec_path')
    mpi_module = xu.get_value_of_key(system_config_root, 'mpi_module')
    return [mpi_module, exec_path]


def get_dicex_base_dir():
    return os.environ['dicex_base_dir']


def get_shell_enviorment_source_cmd():
    '''
    returns command to source the dicex environment 
    '''
    #cmd = ". " + get_dicex_base_dir() + "/" + "dicex_" + get_cluster_name() + ".sh"
    # this is the default <--> TODO: somehow this needs to be created
    cmd = ". ~/versa/env.sh"
    return cmd


def get_db_system_param_value_dict(host_type='standard'):
    db_cfg_elem = xu.get_elems(
        system_config_root, 'cluster/job_queue/' + host_type, uniq=True)
    return xu.XmlDictConfig(db_cfg_elem)


def combinations_with_replacement(iterable, r):
    pool = tuple(iterable)
    n = len(pool)
    for indices in product(range(n), repeat=r):
        if sorted(indices) == list(indices):
            yield tuple(pool[i] for i in indices)


def partition_in_chunks(end, num_chunks):
    k, m = divmod(end, num_chunks)
    return [(i * k + min(i, m), (i + 1) * k + min(i + 1, m)) for i in range(num_chunks)]


def retry_until_timeout(afunc, retry_exception, timeout=5):
    start_time = time.time()
    res = None
    while True:
        try:
            res = afunc()
            print("retry success = ", res)
            break
        except Exception as e:
            elapsed = time.time() - start_time
            print("retrying after = ", elapsed)
            print("exception = ", e)
            if elapsed > timeout:
                break
            time.sleep(1)
    return res


def run_once(f):
    '''
    a decorator to run a function only once
    '''
    def wrapper(*args, **kwargs):
        if not wrapper.has_run:
            wrapper.has_run = True
            return f(*args, **kwargs)

    wrapper.has_run = False
    return wrapper


def is_url(url):
    return urlparse(url).scheme in ("http", "https")


def download_url(url, save_dir=".", char_limit=13):
    filename = None
    with requests.get(url, stream=True) as r:
        hash = sha256(url.encode()).hexdigest()[:char_limit]
        filename = f'{save_dir}/{hash}.html'
        with open(filename, 'wb') as f:
            #shutil.copyfileobj(r.content, f)
            f.write(r.content)
    return filename
