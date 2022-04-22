from addict import Dict
from versa_webapp.analytics_dashboard import actions


model = Dict()
model.session_id = "eggfry"
actions.START_DBSESSION(model)
