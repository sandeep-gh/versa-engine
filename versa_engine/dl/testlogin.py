from core.scripts.pagefromfile import PageFromFileReader, PageFromFileRobot
from pywikibot import config
from pywikibot import config, Site
config.log = ['*']

config.logfilename = "auniqfile"
config.verbose_output = False
print(config.log)



autocreate = False
family = "csvpackdl"
site = Site(code="en", fam=family)

login_user = None
try:
    site.login(autocreate=autocreate)
    login_user = site.user()
except  Exception as e:
    print ("login error", e)
