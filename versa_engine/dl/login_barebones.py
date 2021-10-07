import pywikibot


def login():
    autocreate = False
    site = pywikibot.Site()
    namedict = {site.family.name: {site.code: None}}
    #family = next(iter(namedict.keys()))
    family = "csvpackdl"
    site = pywikibot.Site(code=site.code, fam=family)

    login_user = None
    try:
        site.login(autocreate=autocreate)
        login_user = site.user()
    except  Exception as e:
        print ("login error", e)
        
    return login_user
    

r = login()
print (r)
