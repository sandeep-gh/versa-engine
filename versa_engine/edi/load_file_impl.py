#!/usr/bin/env python
import os
import subprocess
from string import Template
from ..common import utilities
from ..common import metadata_utils as mu
#from osgeo import ogr
from ..controller.appconfig import AppConfig
import shutil


module_dir = os.path.dirname(os.path.realpath(__file__))


def build_indexes(tbl_name, metadata_root, port, pgdb='postgres', schema='public'):
    setenv_path = AppConfig.get_setevn_path()
    create_idx_template = Template(
        ". ${setenv_path}; echo \"create index ${tbl_name}_${col}_idx on ${schema}.${tbl_name}($col)\" | psql -p ${port} ${pgdb}")
    all_keys = mu.get_primary_keys(metadata_root)
    foreign_keys = mu.get_foreign_keys(metadata_root)
    for k in all_keys:
        key = k.lower()
        if key in foreign_keys:
            col = 'f_' + key
        else:
            col = key
        a = locals()
        b = globals()
        a.update(b)
        create_idx_cmd = create_idx_template.substitute(a)
        subprocess.call(create_idx_cmd, shell=True)


def build_primary_key(primary_keys, foreign_keys):
    key_str = ''
    all_keys = primary_keys
    for k in all_keys:
        key = k.lower()
        if key in foreign_keys:
            key = 'f_' + key
        key_str = key_str + "\\'" + key.strip().lower() + "\\',"
    return key_str[:-1]


def build_clone_primary_key(primary_keys, foreign_keys):
    key_str = ''
    all_keys = primary_keys
    for k in all_keys:
        key = k.lower()
        if key in foreign_keys:
            key = 'f_' + key
        key_str = key_str + "\\'" + 'm_' + key.strip().lower() + "\\',"
    return key_str[:-1]


def create_table_from_metadata(metadata_root, wd=None, port=None, pgdb='postgres', schema='public'):
    if wd is not None:
        work_dir = wd
    assert work_dir is not None
    model_name = mu.get_model_name(metadata_root)
    primary_keys = mu.get_primary_keys(metadata_root)
    foreign_keys = mu.get_foreign_keys(metadata_root)
    fh = open(work_dir.rstrip() + "/" + model_name.strip() + ".schema", 'w+')
    mfh = open(work_dir.rstrip() + "/" +
               model_name.strip() + ".mirror_schema", 'w+')

    all_columns = mu.get_columns_and_type(metadata_root)
    for cname, ctype in all_columns:
        if cname and ctype:
            if cname in foreign_keys:
                cname = 'f_' + cname
            print(cname, " ", ctype, file=fh)
            print('m_'+cname, " ", ctype, file=mfh)

    fh.close()
    mfh.close()
    tbl_name = model_name.strip().lower()
    clone_tbl_name = tbl_name + '_mirror'
    clone_model_name = model_name + '_mirror'
    primary_key = build_primary_key(primary_keys, foreign_keys)
    clone_primary_key = build_clone_primary_key(primary_keys, foreign_keys)
    setenv_path = AppConfig.get_setevn_path()
    create_pg_schema_template = Template(
        ". ${setenv_path}; awk -f ${module_dir}/create_pg_schema_v2.awk ${work_dir}/${model_name}.schema |sed \'s/__TBL__/\'${tbl_name}\'/g\' | sed \'s/__SCHEMA__/\'${schema}\'/g\' | psql -p $port ${pgdb}  >> ${work_dir}/psql_out")
    a = locals()
    b = globals()
    a.update(b)
    create_pg_schema_cmd = create_pg_schema_template.substitute(a)
    subprocess.call(create_pg_schema_cmd, shell=True)
    ### adding indexes####
    build_indexes(tbl_name, metadata_root, port)
    # metadata_root.getElementsByTagName('primarykey')[0], foreign_keys)
    #create_clone_pg_tbl_cmd= Template("awk -f ${module_dir}/create_cloned_view.awk ${work_dir}/${model_name}.mirror_schema | sed \'s/__TBL__/\'$tbl_name\'/g\' | sed \'s/__VIEW__/\'${clone_tbl_name}\'/g\' | psql -p $port postgres  >> ${work_dir}/psql_out")
    #create_clone_pg_tbl_str = create_clone_pg_tbl_cmd.substitute(a)
    #subprocess.call(create_clone_pg_tbl_str, shell=True)
    # print create_clone_pg_tbl_str


def ingest_from_url(metadata_cfg, cursor, url, work_dir, port, pgdb='postgres', schema='public', delimiter=None, strict_formatted=False, has_header=False, crop_head=None):
    filepath = utilities.download_url(url, save_dir=work_dir)
    filename = os.path.basename(filepath)
    print("filename = ", filename)
    ingest_from_file(metadata_cfg, cursor, work_dir, filename, work_dir=work_dir, port=port, pgdb=pgdb, schema=schema,
                     delimiter=delimiter, strict_formatted=strict_formatted, has_header=has_header, crop_head=crop_head)

    pass


def ingest_from_file(metadata_cfg=None, cursor=None, ddir=None, fn=None, work_dir=None, port=None, pgdb='postgres', schema='public', delimiter=None, strict_formatted=False, has_header=False, crop_head=None):

    assert work_dir is not None
    assert delimiter in ['comma', 'space', 'pipe']
    assert port is not None
    if ddir == '':
        ddir = "."  # TODO: fix the ugly stuff

    has_header_directive = ''
    metadata_root = mu.read_metadata(metadata_cfg)
    model = mu.get_model_name(metadata_root)
    if delimiter == 'space':
        delimiter_char = " "
    if delimiter == 'pipe':
        delimiter_char = "|"
    if delimiter == 'comma':
        delimiter_char = ","

    print("delimiter_char= ", delimiter_char)
    if strict_formatted is True:
        if crop_head is None:
            prepare_data_template = Template(
                "ln -s ${ddir}/${fn} ${work_dir}/${fn}")
        else:
            prepare_data_template = Template(
                """cat ${ddir}/${fn} | tail -n +${crop_head} >   ${work_dir}/${fn}""")
    else:
        prepare_data_template = Template(
            """cat ${ddir}/${fn} | sed '/^\s*$/d' | tr -d '\\\r'|  grep -v \"^\#\"  | awk 'BEGIN{FS="$delimiter_char"}{line_str="";if (NF > 0) {for (i = 1; i <NF; ++i){line_str=line_str $$i " "};line_str = line_str $$NF; print line_str}}' >  ${work_dir}/${fn} """)
        delimiter_char = ' '  # the delimiter is now space

    prepare_data_cmd_str = prepare_data_template.safe_substitute(locals())
    subprocess.call(prepare_data_cmd_str, shell=True)

    if has_header == True:
        has_header_directive = """,header 'True' """
        #check if first line is #

    copy_cmd_str = Template(
        """copy ${model} FROM '${work_dir}/${fn}' with (FORMAT csv, DELIMITER E'${delimiter_char}' ${has_header_directive})""").substitute(locals())
    print("copy cmd = ", copy_cmd_str)
    cursor.execute(copy_cmd_str)
    subprocess.call(Template("rm  ${work_dir}/${fn}").substitute(
        work_dir=work_dir, fn=fn), shell=True)  # only if not strict formatted


# def ingest_from_file(metadata_file=None, dir=None, fn=None, wd=None, port=None, pgdb='postgres', schema='public', delimiter='tab', strict_formatted=False, has_header=False):

#     if wd is not None:
#         work_dir=wd
#     print "delimiter = ", delimiter
#     metadata_root=mu.read_metadata(metadata_file)
#     model = mu.get_model_name(metadata_root)
#     #TODO: do dos2unix /groups/NDSSL/dicex_external_datasets/Global_Food_Prices.csv

#     copy_cmd_template= Template("sed \'s/__TBL_NAME__/\'${model}\'/g\' ${module_dir}/load_table_from_file.delim_tab.sql.template  | sed \'s|__DIR__|\'$work_dir\'|\' |  sed \'s/__SCHEMA__/\'${schema}\'/g\' | sed \'s/__FN__/\'${fn}\'/\' | sed \'s/__PORT__/\'$port\'/\'| sed \'s/__has_header__/\'${has_header_directive}\'/\' > ${work_dir}/copy.sql.${model}")
#     if delimiter == 'pipe':
#         copy_cmd_template= Template("sed \'s/__TBL_NAME__/\'${model}\'/g\' ${module_dir}/load_table_from_file_delim_pipe.sql.template  | sed \'s|__DIR__|\'$work_dir\'|\' | sed \'s/__FN__/\'${fn}\'/\' |  sed \'s/__SCHEMA__/\'${schema}\'/g\' | sed \'s/__PORT__/\'$port\'/\'| sed \'s/__has_header__/\'${has_header_directive}\'/\' > ${work_dir}/copy.sql.${model}")
#     if delimiter == 'comma':
#         copy_cmd_template= Template("sed \'s/__TBL_NAME__/\'${model}\'/g\' ${module_dir}/load_table_from_file_delim_comma.sql.template  | sed \'s|__DIR__|\'$work_dir\'|\' | sed \'s/__FN__/\'${fn}\'/\' | sed \'s/__SCHEMA__/\'${schema}\'/g\' |  sed \'s/__PORT__/\'$port\'/\'| sed \'s/__has_header__/\'${has_header_directive}\'/\' > ${work_dir}/copy.sql.${model}")
#     if delimiter == 'space':
#         copy_cmd_template= Template("sed \'s/__TBL_NAME__/\'${model}\'/g\' ${module_dir}/load_table_from_file_delim_space.sql.template  | sed \'s|__DIR__|\'$work_dir\'|\' | sed \'s/__FN__/\'${fn}\'/\' |  sed \'s/__SCHEMA__/\'${schema}\'/g\' | sed \'s/__PORT__/\'$port\'/\'| sed \'s/__has_header__/\'${has_header_directive}\'/\' > ${work_dir}/copy.sql.${model}")
#     if delimiter == 'tab':
#         copy_cmd_template= Template("sed \'s/__TBL_NAME__/\'${model}\'/g\' ${module_dir}/load_table_from_file_delim_space.sql.template  | sed \'s|__DIR__|\'$work_dir\'|\' | sed \'s/__FN__/\'${fn}\'/\' |  sed \'s/__SCHEMA__/\'${schema}\'/g\' | sed \'s/__PORT__/\'$port\'/\'| sed \'s/__has_header__/\'${has_header_directive}\'/\' > ${work_dir}/copy.sql.${model}")

#     #prepare_data_template= Template("cat ${dir}/${fn} | grep -v \"^\#\" | sed  \'s/  *$$//\' |   column -t -s $$\'\\t\'   | awk \'gsub(/\\s+/,\"\\t\")'  > ${work_dir}/${fn}")


#     print "from ingest from file ", strict_formatted, " ", delimiter
#     prepare_data_template = None
#     if strict_formatted is True:
#         prepare_data_template= Template("ln -s ${dir}/${fn} ${work_dir}/${fn}")
#     else:

#         if delimiter == 'comma':
#             prepare_data_template= Template("cp ${dir}/${fn} ${work_dir}/${fn}.c; dos2unix ${work_dir}/${fn}.c; cat ${dir}/${fn}.c | grep -v \"^\#\" | sed  \'s/  *$$//\'   | awk \'{if ($0 ~ / |\\s+/){gsub(/\\s+/,\"\\t\",$0);print $0}else{print $0}}'  > ${work_dir}/${fn}")
#         if delimiter == 'pipe':
#             prepare_data_template= Template("cat ${dir}/${fn} | grep -v \"^\#\" | sed  \'s/  *$$//\' > ${work_dir}/${fn}")
#         if delimiter == 'space':
#             prepare_data_template= Template("""cp ${dir}/${fn} ${work_dir}/${fn}.c; dos2unix ${work_dir}/${fn}.c; cat ${work_dir}/${fn}.c | grep -v \"^\#\" | sed  \'s/  *$$//g\' | sed  \'s/  */ /g\' | sed  \'/^$$/d\'  > ${work_dir}/${fn}""")
#         if delimiter == 'tab':
#             prepare_data_template= Template("cat ${dir}/${fn} | grep -v \"^\#\" | sed  \'s/  *$$//\' > ${work_dir}/${fn}")

#     assert prepare_data_template is not None

#     copy_exec_template = Template("psql -p ${port} ${pgdb} < ${work_dir}/copy.sql.${model} >> ${work_dir}/psql_out")
#     has_header_directive=''
#     if has_header==True:
#         has_header_directive = """\"\, header \\'True\\'\""""

#     a = locals()
#     b = globals()
#     a.update(b)

#     copy_cmd_str= copy_cmd_template.substitute(a)
#     copy_cmd_str = Template(copy_cmd_str).substitute(a)


#     #We are using sed to prepare the copy command -- this is mostly for legacy reason
#     subprocess.call(copy_cmd_str, shell=True)

#     prepare_data_cmd_str =  prepare_data_template.safe_substitute(a)

#     print prepare_data_cmd_str
#     subprocess.call(prepare_data_cmd_str, shell=True)

#     copy_exec_str = copy_exec_template.substitute(a)
#     print copy_exec_str
#     subprocess.call(copy_exec_str, shell=True)


def get_shape_file_table_name(rasterFile):
    reader = ogr.Open(rasterFile + ".shp")  # No need to specify suffix
    x = reader.GetLayer(0)
    return x.GetName().lower()  # it seems ogr returns name with first letter captalized


def create_shape_table_from_file(port, rasterFile, model_name=None):
    rename_cmd = ""
    if model_name is not None:
        tbl_name = get_shape_file_table_name(rasterFile)

        rename_cmd = Template(
            '''sed \'s/$tbl_name/$model_name/g\'''').substitute(locals())
    remove_insert_cmd = "grep -v \"INSERT\" "
    setenv_path = AppConfig.get_setevn_path()
    subprocess.call('''. ${setenv_path}; shp2pgsql -W 'latin1' -I  '''+rasterFile+"|" + remove_insert_cmd +
                    "|" + rename_cmd + "|" + ''' psql -p '''+str(port)+''' postgres''', shell=True)


def load_shape_data_from_file(port, rasterFile, model_name=None):
    '''
    load raster file with name=model_name 
    '''
    rename_cmd = ""
    if model_name is not None:
        tbl_name = get_shape_file_table_name(rasterFile)
        rename_cmd = Template(
            '''sed \'s/$tbl_name/$model_name/g\'''').substitute(locals())

    keep_insert_cmd = "grep  \"INSERT\" "
    setenv_path = AppConfig.get_setevn_path()

    subprocess.call('''. ${setenv_path}; shp2pgsql -W 'latin1' -I  '''+rasterFile+"|" + keep_insert_cmd +
                    "|" + rename_cmd + "|" + ''' psql -p '''+str(port)+''' postgres''', shell=True)
    #subprocess.call('''shp2pgsql -W 'latin1' -I  '''+rasterFile+"|"+ rename_cmd+ "> out", shell=True)


def get_raster_file_table_name(rasterFile):
    return get_shape_file_table_name(rasterFile)  # yet to be tested


def create_raster_table_from_file(port, rasterFile, model_name=None):
    rename_cmd = ""
    if model_name is not None:
        tbl_name = get_shape_file_table_name(rasterFile)
        rename_cmd = Template(
            '''sed \'s/$tbl_name/$model_name/g\'''').substitute(locals())
    remove_insert_cmd = "grep -v \"INSERT\" "
    setenv_path = AppConfig.get_setevn_path()
    subprocess.call('''. ${setenv_path}; raster2pgsql -W 'latin1' -I  '''+rasterFile+"|" + remove_insert_cmd +
                    "|" + rename_cmd + "|" + ''' psql -p '''+str(port)+''' postgres''', shell=True)


def load_raster_data_from_file(port, rasterFile, model_name=None):
    '''
    load raster file with name=model_name 
    '''
    rename_cmd = ""
    if model_name is not None:
        tbl_name = get_shape_file_table_name(rasterFile)
        rename_cmd = Template(
            '''sed \'s/$tbl_name/$model_name/g\'''').substitute(locals())
    keep_insert_cmd = "grep  \"INSERT\" "
    setenv_path = AppConfig.get_setevn_path()
    subprocess.call('''. ${setenv_path}; raster2pgsql -W 'latin1' -I  '''+rasterFile+"|" + keep_insert_cmd +
                    "|" + rename_cmd + "|" + ''' psql -p '''+str(port)+''' postgres''', shell=True)

# def load_raster_file(port, rasterFile):
#    subprocess.call("raster2pgsql -I  -C "+rasterFile+" |psql -p "+str(port)+" postgres", shell=True)




# def save_file(url, content, save_dir=".", char_limit=13):
#     # hash url as sha256 13 character long filename
#     hash = sha256(url.encode()).hexdigest()[:char_limit]
#     filename = f'{save_dir}/{hash}.html'
#     # 93fb17b5fb81b.html
#     with open(filename, 'w') as f:
#         f.write(content)
#     # set url attribute
#     os.setxattr(filename, 'user.url', url.encode())
#     return filename


