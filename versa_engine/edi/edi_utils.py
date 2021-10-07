
    

def create_oracle_fdw(data_server):
    create_oracle_fdw_template_str= open(create_oracle_fdw.cmd.template, "r").read()
    a = locals()
    b = globals()
    a.update(b)
    create_oracle_fdw_cmd_str = create_oracle_fdw_template_str.substitute(a) #ignore shell variables
    subprocess.call(create_oracle_fdw_cmd_str, shell=True)
