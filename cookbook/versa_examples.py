import versa_api as vapi

def build_age_distribution():
    vapi.aggregate(session, population, 

def compare_age_distribution(session, census_distribution, population):
    '''
    census_distribution(age1, age2, male, female)
    population(pid, f_householdlid, daytimelid, age, gender
    analysis: compare age distribution in the census vs. the synthetic population
    '''
    
    #############################################################
    ### get census popsz by agebins -- sum males and females
    #############################################################
    census_popsz_by_age_bins = vapi.sum_attrs(session, census_distribution, 'male', 'female', 'census_sz')
    
    #############################################################
    ### break population by age bins defined by ['age1', 'age2']
    #############################################################
    synpop_by_agebins = vapi.proj(session, vapi.fuseRange(session, population, popbyageandsex, 'age', 'age1', 'age2'), ['age', 'age1', 'age2', 'census_sz'])
    
    #############################################################
    ### count popsize in each age bin -- aggregate by ['age1', 'age2'] 
    #############################################################
    popsz_by_agebins = vapi.aggregate(session, synpop_by_agebins, ['age1', 'age2'], 'syn_popsz')

    #############################################################
    ### join the census and synthetic popsz
    #############################################################
    census_popsz_by_agebins = []
    syn_popsz_by_agebins  = []
    for rec in vapi.scan(session, popsz_by_agebins):
        age_lb = rec.age1
        age_ub = rec.age2
        census_popsz_by_agebins.append(rec.census_sz)
        syn_popsz_by_agebins.append(rec.syn_popsz)
	return [census_popsz_by_agebins, syn_popsz_by_agebins]
