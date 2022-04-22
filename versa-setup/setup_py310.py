import importlib
import os
import time
def pip_install(pkg, module, pip_path=""):
    try:
        importlib.import_module(module)
        print ("already installed")
    except Exception as e:
        os.system(f"{pip_path}/pip3 install {pkg}")
        time.sleep(1)

try:
    import uvicorn
except Exception as e:
    pip_install("justpy", "justpy")
    os.system("pip3 uninstall justpy") # we only need justpy dependency
    


try:

    import sqlalchemy
except Exception as e:
    os.system("pip3 install sqlalchemy-postgres-copy PyShp scipy matplotlib shapely requests  seaborn multiprocess geoalchemy2 RPyC  strconv urllib3")
    # pip3 install  mwparserfromhell pywikibot wheel sqlalchemy aenum line_profiler   enum_switch justpy requests epc  autopep8 flake8 jedi virtualenv grip intervals setuptools
    # tabulate

    print("failed import ", e)


if not os.path.exists(f"{base_dir}/{project_root}/justpy"):
    os.chdir(f"{base_dir}/{project_root}")
    Repo.clone_from("https://github.com/elimintz/justpy", "justpy")
