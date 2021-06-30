
class data_server:
    def __init__(self, name, user, url, password, server_type, database=None):
        self.name=name 
        self.user=user
        self.url=url
        self.password=password
        self.server_type=server_type
        self.database = database
