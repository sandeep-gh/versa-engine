import xml.etree.ElementTree as ET
import xml.etree as etree
from xml.dom import minidom
import os
import sys
import subprocess
from string import Template
import xml.etree.ElementInclude
import xmldict
import collections
import traceback

# element is xml tree node
# keys are elements that are queried
# value is string literal

module_dir = os.path.dirname(os.path.realpath(__file__))


def check_if_xml_tree(data):
    if isinstance(data, etree.ElementTree.Element):
        return True
    if isinstance(data, etree.ElementTree.ElementTree):
        return True

    return False


def read_string(xml_str):
    xmldoc = ET.fromstring(xml_str)
    xml.etree.ElementInclude.include(xmldoc)
    xml.etree.ElementInclude.include(xmldoc)
    return xmldoc


def read_file(xml_fn):
    if check_if_xml_tree(xml_fn):
        return xml_fn
    try:
        xmldoc = ET.parse(xml_fn)
    except Exception as e:
        return None

    root = xmldoc.getroot()
    xml.etree.ElementInclude.include(root)
    xml.etree.ElementInclude.include(root)
    return root


def tostring(root):
    root_txt = ET.tostring(root)
    root_txt = minidom.parseString(root_txt).toprettyxml()
    return root_txt
    # return ET.dump(root)


def get_attr_list(root):
    # <root attr1='', attr2=''></root>
    return root.keys()


def get_value(root):
    if root is None:
        return None

    try:
        val = root.text.strip()
        return val
    except:
        return root.text


def get_elem_iter(root, attrname, path_prefix=".//"):
    '''
    Not sure what the return type should be for non-existing element
    Check all places where this function is begin called
    '''
    all_attr_elems = root.findall(path_prefix+attrname)

    for elem in all_attr_elems:
        yield elem


def get_elems(root, attrname, path_prefix=".//", uniq=False, error_if_not_found=True):
    '''
    Not sure what the return type should be for non-existing element
    Check all places where this function is begin called
    error_if_not_found: if the attribute is not found than an error gets raised. 
    the expected usage is to check for has_key before calling this function.
    '''
    all_attr_elems = root.findall(path_prefix+attrname)

    if len(all_attr_elems) == 0:
        assert None  # raise error
        return None
    if uniq:
        assert(len(all_attr_elems) == 1)
        return all_attr_elems[0]
    return all_attr_elems


def get_value_elems(root, attrname):
    values = []
    for elem in get_elems(root, attrname):
        values.append(get_value(elem))
    return values

# this is the most commonly used one -- sort of like key-value pair


def get_value_elem(root, attrname, path_prefix='.//'):
    '''
    give value for attrname
    '''
    elem = get_elems(root, attrname, path_prefix=path_prefix, uniq=True)
    return get_value(elem)


def get_value_of_key(root=None, key=None, path_prefix='.//'):
    '''
    <key>value<key>
    key should be unique in the xml of the root
    '''
    elem = get_elems(root, key, path_prefix=path_prefix, uniq=True)
    return get_value(elem)


# this is the one we use most often
# <attr>value<attr9>
def get_value_by_attr(root, attrname):
    elem = get_elems(root, attrname, uniq=True)
    if elem is None:
        return None
    # when an elem is created at runtime
    # the xml keeps the type info
    # else its all string
    # when docking..lets make it a string
    try:
        res = elem.text.strip()
    except:
        res = elem.text
    return res


def has_key(root, attr_path, path_prefix='./'):
    '''
    find the key anywhere in the doc.

    a key is an xml item, specifically
    the label of the xml item. for e.g.
    in this fragment 
    <alpha>
       <beta>iota</beta>
    </alpha>
    Here "alpha/beta" is the key.


    '''
    elem = root.findall(path_prefix + attr_path)
    if not elem:
        return False
    return True


# <elemn><key>value</key></elemn>
def get_elem_by_key_value(xmldoc, elemn, key, value, uniq=False):
    all_elems = xmldoc.findall('.//'+elemn+'[' + key + '=\'' + value+'\']')
    if uniq:
        assert(len(all_elems) == 1)
        return all_elems[0]
    return all_elems


def get_uniq_elem_by_key_value(xmldoc, elemn, key, value):
    return get_elem_by_key_value(xmldoc, elemn, key, value, uniq=True)


def get_elems_by_parent_child(root, parent, child):
    # get all roots that are of type <parent><child></child></parent>
    return root.findall(".//"+parent+"/"+child)


# Give that element whose parent=parent and has one child element as <key>value</key>
def get_elems_by_parent_child_key_value(root, parent, child, key, value, uniq=False):
    # <parent>
    #  <child>
    #    <key>value</key>
    #  </child>
    # </parent>
    xpath_str = ".//"+parent+"/"+child+"["+key+"=\'"+value+"\']"
    childnodes = root.findall(xpath_str)
    if uniq:
        if(len(childnodes) == 0):
            assert(0)
        assert(len(childnodes) == 1)
        return childnodes[0]
    return childnodes


def gen_node(node_label, node_text):
    '''
    create a new node
    '''
    node = ET.Element(node_label)
    node.text = node_text
    return node


def dock_elem_value(cfg_root=None, dock_path=None, elem_name=None, elem_value=None):
    node = ET.Element(elem_name)
    node.text = str(elem_value)
    if dock_path is not None:
        dock_elem = get_elems(cfg_root, dock_path, uniq=True)
        dock_elem.append(node)
    else:
        cfg_root.append(node)


def append_elem(dock_root, elem_root):
    '''
    dock_root is the master xml element
    elem_root is the xml element to be added to dock_root
    '''
    dock_root.append(elem_root)


def update_elem_value(cfg_root=None, elem_label=None, elem_text=None):
    elem = get_elems(cfg_root, elem_label, uniq=True)
    elem.text = elem_text
    return


class XmlListConfig(list):
    def __init__(self, aList):
        for element in aList:
            if element is not None:
                # treat like dict
                if len(element) == 1 or element[0].tag != element[1].tag:
                    self.append(XmlDictConfig(element))
                # treat like list
                elif element[0].tag == element[1].tag:
                    self.append(XmlListConfig(element))
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)


class XmlDictConfig(dict):
    '''
    Example usage:

    >>> tree = ElementTree.parse('your_file.xml')
    >>> root = tree.getroot()
    >>> xmldict = XmlDictConfig(root)

    Or, if you want to use an XML string:

    >>> root = ElementTree.XML(xml_string)
    >>> xmldict = XmlDictConfig(root)

    And then use xmldict for what it is... a dict.
    '''

    def __init__(self, parent_element):
        if parent_element.items():
            self.update(dict(parent_element.items()))
        for element in parent_element:
            if len(element):
                # treat like dict - we assume that if the first two tags
                # in a series are different, then they are all different.
                if len(element) == 1 or element[0].tag != element[1].tag:
                    aDict = XmlDictConfig(element)
                # treat like list - we assume that if the first two tags
                # in a series are the same, then the rest are the same.
                else:
                    # here, we put the list in dictionary; the key is the
                    # tag name the list elements all share in common, and
                    # the value is the list itself
                    aDict = {element[0].tag: XmlListConfig(element)}
                # if the tag has attributes, add those to the dict
                if element.items():
                    aDict.update(dict(element.items()))
                self.update({element.tag: aDict})
            # this assumes that if you've got an attribute in a tag,
            # you won't be having any text. This may or may not be a
            # good idea -- time will tell. It works for the way we are
            # currently doing XML configuration files...
            elif element.items():
                self.update({element.tag: dict(element.items())})
            # finally, if there are no child tags and no attributes, extract
            # the text
            else:
                self.update({element.tag: element.text})


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def create_dict_param_attr(xml_root=None):
    nested_xml_dict = xmldict.xml_to_dict(xml_root)
    flat_xml_dict = flatten(nested_xml_dict, sep='/')
    return flat_xml_dict


def merge_xml(xml1_path, xml2_path, xmlo_path):
    global module_dir
    with open("tmp.xml", "w+") as fh:
        fh.write(Template("""<?xml version="1.0"?>
<merge xmlns="http://informatik.hu-berlin.de/merge">
  <file1>${xml1_path}</file1>
  <file2>${xml2_path}</file2>
</merge>""").substitute(locals()))
    a = locals()
    a.update(globals())
    merge_cmd = Template(
        "xsltproc ${module_dir}/merge.xslt tmp.xml > ${xmlo_path}").substitute(locals())
    subprocess.call(merge_cmd, shell=True)
