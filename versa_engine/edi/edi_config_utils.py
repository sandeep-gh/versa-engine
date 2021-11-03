from versa_engine.common import xmlutils as xu
from versa_engine.edi.data_server import data_server
from versa_engine.common import metadata_utils as mu


def read_config_file(cfg_fn):
    return xu.read_file(cfg_fn)


def get_data_servers(root, server_type):
    data_server_elems = xu.get_elem_by_key_value(
        root, 'data_server', 'server_type', server_type)
    if data_server_elems is None:
        return None
    data_servers = {}
    for ds in data_server_elems:
        database = None
        if xu.get_value_by_attr(ds, 'server_type') == 'postgres':
            database = 'postgres'  # the default database
            if xu.has_key(ds, 'database'):
                database = xu.get_value_by_attr(ds, 'database')
        dso = data_server(xu.get_value_by_attr(ds, 'name'), xu.get_value_by_attr(ds, 'user'), xu.get_value_by_attr(
            ds, 'url'), xu.get_value_by_attr(ds, 'password'), xu.get_value_by_attr(ds, 'server_type'), database)
        data_servers[xu.get_value_by_attr(ds, 'name')] = dso
    return data_servers


def get_remote_tables(iroot, data_servers):
    """ get all tables from the config file that reside in one of the data servers

    :param iroot: The config file -- read as an xml element
    :param data_servers: a dictionary of name to data servers 
    :returns: list of tables expressed as [schema, tblname]
    """
    if not xu.has_key(iroot, 'remote_tables/remote_table', path_prefix='.//'):
        return None

    table_elems = xu.get_elems(iroot, 'remote_tables/remote_table')
    tables = []

    for tbl in table_elems:
        tserver = xu.get_value_by_attr(tbl, 'server')
        if tserver in data_servers:
            server_type = data_servers[tserver].server_type
            tschema = xu.get_value_by_attr(tbl, 'schema')
            tname = xu.get_value_by_attr(tbl, 'name')

            model_name = tname  # default
            if xu.has_key(tbl, 'model_name'):
                model_name = xu.get_value_by_attr(tbl, 'model_name')

            metadata = xu.get_value_by_attr(tbl, 'metadata')
            # assuming metadata is <model_name>.md is missing
            if metadata is None:
                metadata = model_name + ".md"

            database = None
            if server_type == 'postgres':
                database = 'postgres'  # default
                if data_servers[tserver].database is not None:
                    database = data_servers[tserver].database
                if xu.has_key(tbl, 'database'):
                    database = xu.get_value_by_attr(tbl, 'database')
            tables.append([tserver, tschema, tname,
                           metadata, database, model_name])

    return tables


def get_files(iroot):
    if not xu.has_key(iroot, 'files/file'):
        return

    file_elems = xu.get_elems(iroot, 'files/file', path_prefix='./')
    if file_elems is None:
        return None

    all_files = []
    for fe in file_elems:
        fe_dict = {'metadata': None, 'base_tbl': None, 'location': None, 'model_name': None, 'delimiter': None,
                   'locations': None, 'filetype': None, 'strict_formatted': None, 'has_header': False, 'schema': None}
        # this was a shortcut approach; which now is a pain
        # fe_dict.update(xu.XmlDictConfig(fe))
        if xu.has_key(fe, 'metadata'):
            fe_dict['metadata'] = xu.get_value_elem(fe, 'metadata')
        if xu.has_key(fe, 'base_tbl'):
            fe_dict['base_tbl'] = xu.get_value_elem(fe, 'base_tbl')
        if xu.has_key(fe, 'location'):
            fe_dict['location'] = xu.get_value_elem(
                fe, 'location', path_prefix='./')
        if xu.has_key(fe, 'locations'):
            fe_dict['locations'] = xu.get_elems(fe, 'locations', uniq=True)

        # get model_name
        if xu.has_key(fe, 'model_name'):
            fe_dict['model_name'] = xu.get_value_elem(fe, 'model_name')
        if fe_dict['model_name'] is None:
            # use metadata  model name
            metadata_root = xu.read_file(fe_dict['metadata'])
            fe_dict['model_name'] = mu.get_model_name(metadata_root)
            assert fe_dict['model_name'] is not None

        if xu.has_key(fe, 'delimiter'):
            fe_dict['delimiter'] = xu.get_value_elem(fe, 'delimiter')
        if xu.has_key(fe, 'filetype'):
            if xu.has_key(fe, 'filetype/filetype'):
                # contents within <filetype> elem -- namely delimiter, header, strict_formatted
                filetype_param_value = parse_filetype_elem(
                    xu.get_elems(fe, 'filetype', path_prefix='./', uniq=True))
                fe_dict.update(filetype_param_value)
            else:
                # non csv file
                fe_dict['filetype'] = xu.get_value_elem(fe, 'filetype')
        assert 'filetype' in fe_dict
        assert fe_dict['filetype'] is not None
        all_files.append(fe_dict)
    return all_files


def get_shape_files(iroot):
    all_files = get_files(iroot)
    shape_files = []
    for afile in all_files:
        if afile['filetype'] == 'shape':
            shape_files.append(afile)
    print("shape_files = ", shape_files)
    return shape_files


def parse_filetype_elem(filetype_elem=None):
    fe_dict = {'delimiter': None, 'strict_formatted': False,
               'has_header': False, 'crop_head': None}
    if xu.has_key(filetype_elem, 'filetype'):
        fe_dict['filetype'] = xu.get_value_elem(filetype_elem, 'filetype')
    if xu.has_key(filetype_elem, 'strict_formatted'):
        fe_dict['strict_formatted'] = xu.get_value_elem(
            filetype_elem, 'strict_formatted') == 'True'
    if xu.has_key(filetype_elem, 'delimiter'):
        fe_dict['delimiter'] = xu.get_value_elem(filetype_elem, 'delimiter')
    # if xu.has_key(filetype_elem, 'has_header'):
    #     fe_dict['has_header'] = xu.get_value_elem(
    #         filetype_elem, 'has_header') == 'True'
    if xu.has_key(filetype_elem, 'crop_head'):
        fe_dict['crop_head'] = int(
            xu.get_value_elem(filetype_elem, 'crop_head')) + 1

        if fe_dict['crop_head'] == 0:
            fe_dict['crop_head'] = None

        if fe_dict['crop_head'] == 2:
            fe_dict['has_header'] = True
            fe_dict['crop_head'] = None
    return fe_dict


def gen_edconfig_elem():
    edcfg_root = xu.read_string('''<edconfig><files></files></edconfig>''')
    files_elem = xu.get_elems(edcfg_root, 'files', uniq=True)
    return [edcfg_root, files_elem]


def add_file_elem_to_edcfg(edcfg_root, file_elem):
    files_elem = xu.get_elems(edcfg_root, 'files', uniq=True)
    files_elem.append(file_elem)
    pass
