import common.metadata_utils as mu
import rmo.versa_api_meta as vam
from addict import Dict
import rmo.utils as vu
import rmo.versa_impl as vi
import dl
import common.xmlutils as xu
run_dir = "/tmp/tmp5gbgkb_2/"
model = Dict(track_changes=True)

model.dbsession = vu.build_session("mydbsession", session_run_dir=run_dir)

# /home/kabira/Datasets/datalouge/bankinfo.csvpack
# vi.build_orms("/home/kabira/Datasets/datalouge/bankinfo.csvpack")

#vi.import_model(model_name, model_dir=None)
dl.login()
cfgxml = dl.get_page_text("Countyexpenditure.csvpack")

cfgroot = xu.read_string(cfgxml)

dbconn, dbsession = vi.init("testsession", "/tmp/tmp5gbgkb_2")
all_models = vi.build_orms(cfgroot, work_dir="/tmp/tmp5gbgkb_2")

m = vu.import_model(all_models[0])
res = vam.get_cols_nametype(dbsession, m)

mdroot = mu.read_metadata("dl://bankinfo.md")


run_dir = "/tmp/tmp5gbgkb_2/"
active_rmos = [vu.import_model("countyexpenditure", run_dir)]
