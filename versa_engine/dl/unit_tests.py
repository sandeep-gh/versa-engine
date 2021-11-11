from versa_engine.dl.dl import login, save_page_text, get_page_text

cfgdata = "testing something something"

login()
rr = get_page_text("newnewnew.md")
save_page_text(cfgdata, "newnewnew.md")
