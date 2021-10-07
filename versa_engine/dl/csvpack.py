from scripts.pagefromfile import PageFromFileReader, PageFromFileRobot
from pywikibot import config, Site, Page
from scripts.login import main as loginmain
import os
#config.log = ['*']

#config.logfilename = "auniqfile"
#config.verbose_output = False
#print(config.log)
site = None
def login():
    global site
    autocreate = False
    family = "csvpackdl"
    site = Site(code="en", fam=family)

    login_user = None
    try:
        site.login(autocreate=autocreate)
        login_user = site.user()
    except  Exception as e:
        print ("login error", e)
    return login_user
    


def add_csvpack_to_wiki(cfgdata, title, tmp_dir=""):
    print ("in add_csvpack_to_wiki:", tmp_dir)
    if not os.path.exists("/tmp/" + tmp_dir):
        os.mkdir("/tmp/" + tmp_dir)
    filename = "/tmp/" + tmp_dir + "/" + title   # genrandom
    print ("add_csvpack to dl = ", filename)
    with open(filename, "w") as fh:
        fh.write("{{-start-}}" + cfgdata + "{{-stop-}}")

    r_options = {}
    r_options['title'] = title
    options = {}
    options['summary'] = "bot from versa"
    options['always'] = True
    reader = PageFromFileReader(filename, **r_options)
    bot = PageFromFileRobot(generator=reader, **options)
    ret_code = bot.run()
    print ("bot ret_code = ", ret_code)


def get_page_text(title):
    page = Page(site, title)
    return page.text


def edit_page_text(text, title):
    page = Page(site, title)
    page.text = text
    page.save("updated existing page")
