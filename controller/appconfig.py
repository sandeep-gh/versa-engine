import common.utilities as utilities
import common.xmlutils as xu


class AppConfig:
    system_config_root = utilities.get_system_config_root()
    port_server_ip = xu.get_value_of_key(system_config_root, 'port_server_ip')
    cluster_name = xu.get_value_of_key(system_config_root, 'cluster_name')
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
