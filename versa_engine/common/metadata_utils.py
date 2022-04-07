from versa_engine.common import xmlutils as xu
#from versa_engine.dl import dl
# element,


def read_metadata(metadata_fn):
    if xu.check_if_xml_tree(metadata_fn):
        return metadata_fn

    #TODO: use of new dl library
    if metadata_fn[0:5] == "dl://":
        #read metadata from dl
        mdxml = dl.get_page_text(metadata_fn[5:])
        return xu.read_string(mdxml)
    return xu.read_file(metadata_fn)

def set_model_name(metadata_root, model_name):
    model_name_elem = xu.get_elems(metadata_root, 'model', uniq=True)
    model_name_elem.text = model_name

def get_model_name(metadata_root):
    #<model>modelname</model>
    model_name=xu.get_value_by_attr(metadata_root, 'model')
    return model_name


def get_columns_and_type(metadata_root):
    #<columns><column></column>
    #         <column></column>
    #</columns>
    all_columns=[]
    for col_elem in xu.get_elems_by_parent_child(metadata_root, 'columns', 'column'):
        col_name=xu.get_value_by_attr(col_elem,'name')
        col_type=xu.get_value_by_attr(col_elem,'type')
        all_columns.append([col_name, col_type])
    return all_columns

def is_fkey(metadata_root, column_name):
    xroot=xu.get_elems_by_parent_child_key_value(metadata_root, 'columns', 'column', 'name', column_name, True)
    if 'fkey' in xu.get_attr_list(xroot):
        return True
    return False
    

def get_primary_keys(metadata_root):
    all_primary_keys=[]
    key_xroots=xu.get_elems_by_parent_child(metadata_root, 'primarykey', 'key')
    for key_xroot in key_xroots:
        pk_text=xu.get_value(key_xroot)
        all_primary_keys.append(pk_text)
    return all_primary_keys

def get_foreign_keys(metadata_root):
    all_foreign_keys=[]
    for col_name, col_type in get_columns_and_type(metadata_root):
        if is_fkey(metadata_root, col_name):
            all_foreign_keys.append(col_name)

    return all_foreign_keys
 

def get_delimiter(metadata_root):
    elem = xu.get_elems(metadata_root, 'delimiter', uniq=True)
    if elem is None:
        return None
    return xu.get_value_elem(metadata_root, 'delimiter')


def get_header(metadata_root):
    elem = xu.get_elems(metadata_root, 'header', uniq=True)
    if elem is None:
        return False
    return xu.get_value_elem(metadata_root, 'header')
