from addict import Dict

from versa_engine.controller import frontend_controller


def START_DBSESSION(model, arg=None):
    dbsession_id = model.session_id
    run_dir = "~/DrivingRange/versa_runs/" + dbsession_id
    run_dir = "/tmp/tmp5gbgkb_2/"
    model.run_dir = run_dir
    model.connCtx_proxy = frontend_controller.launchdbjob(
        dbsession_id, run_dir=model.run_dir)
    model.op = "Start dbsession"
    if model.connCtx_proxy is not None:
        model.noticeboard_message = "successfully launched new database session"
    else:
        model.noticeboard_message = " Unable to launch dbsession: this event is logged"
    
    pass


model = Dict()
model.session_id = "testsession"
START_DBSESSION(model)

