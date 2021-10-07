import logging
import pywikibot
pywikibot.output('This will initialize the logger')
logger = logging.getLogger('pywiki')
logger.setLevel(logging.ERROR)

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
    

def get_page_text(title):
    page = Page(site, title)
    return page.text


def save_page_text(text, title):
    '''
    create new page or edit existing page
    '''
    page = Page(site, title)
    page.text = text
    page.save("updated existing page")
