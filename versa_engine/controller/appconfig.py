import versa_engine.common.utilities as utilities
import versa_engine.common.xmlutils as xu


class AppConfig:
    system_config_root = utilities.get_system_config_root()
    port_server_ip = xu.get_value_of_key(system_config_root, 'port_server_ip')
    cluster_name = xu.get_value_of_key(system_config_root, 'cluster_name')
    setenv_path = xu.get_value_of_key(system_config_root, 'setenv_path')
    pybin_path  = xu.get_value_of_key(system_config_root, 'pybin_path')


    frontend_proxy_port = 45239  # only one proxy runs on a node

    @staticmethod
    def get_port_server_ip():
        return AppConfig.port_server_ip

    @staticmethod
    def get_cluster_name():
        return AppConfig.cluster_name

    @staticmethod
    def get_frontend_proxy_port():
        return AppConfig.frontend_proxy_port

    @staticmethod
    def get_setevn_path():
        return AppConfig.setenv_path


    @staticmethod
    def get_pybin_path():
        return AppConfig.pybin_path

    @staticmethod
    def get_versa_node(remotenode_name="pi4bsdm1"):
        ans = xu.get_elems_by_parent_child_key_value(system_config_root,
                                               "versa_nodes", "node", "name", "pi4bsdm1", uniq=True)
        return ans.ip, ans.port
        
        
    

    
