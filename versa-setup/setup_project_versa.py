import importlib
import os
import time

import subprocess




    
def pip_install(pkg, module, pybindir=""):
        all_pkgs  = [_.decode('ascii').lower() for _ in subprocess.check_output([f"{pybindir}pip3", 'list']).split()]
        if pkg in all_pkgs:
            print ("pkg= ", pkg, " is present")
            return "already present"
        else:
            os.system(f"{pybindir}pip3 install {pkg}")
            return "installed"
        
pip_install("gitpython", "git")
pip_install("wget", "wget")



from git import Repo
import subprocess
import wget

base_dir = os.getcwd()
base_dir = "/home/kabira/DrivingRange/"
project_root = f"{base_dir}/project_versa"
if not os.path.exists(project_root):
    os.makedirs(project_root)
os.chdir(project_root)
if not os.path.exists("downloads"):
    os.makedirs("downloads")

if not os.path.exists("Builds"):
    os.makedirs("Builds")
    
os.chdir("downloads")

#https://www.python.org/ftp/python/3.10.0/Python-3.10.0.tar.xz
version = "3.10.0"

if not os.path.exists(f"Python-{version}.tar.xz"):
    fn = wget.download(
        f"https://www.python.org/ftp/python/{version}/Python-{version}.tar.xz")
if not os.path.exists(f"Python-{version}"):
    os.system(f"tar xvf Python-{version}.tar.xz")


os.chdir("../Builds")
if not os.path.exists(f"Python-{version}"):
    os.makedirs(f"Python-{version}")
    os.chdir(f"../downloads/Python-{version}")
    build_cmd = f"""
       ./configure --prefix={project_root}/Builds/Python-{version} --enable-optimizations --with-lto --with-computed-gotos --with-system-ffi
make -j 4
make install
    """
    print (build_cmd)
    os.system(build_cmd)
    
pybindir = f"{project_root}/Builds/Python-{version}/bin/"


pip_install("uvicorn", "uvicorn", pybindir)

[pip_install(_, _, pybindir) for _ in   "wheel sqlalchemy xmldict tabulate pandas sqlalchemy-postgres-copy PyShp scipy matplotlib shapely requests pyproj seaborn multiprocess geoalchemy2 RPyC  strconv  urllib3 demjson3 jsbeautifier  dill".split()]



# if not os.path.exists(f"{project_root}/justpy"):
#     os.chdir(f"{project_root}")
#     Repo.clone_from("https://github.com/elimintz/", "justpy")
