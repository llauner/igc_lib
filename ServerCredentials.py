class ServerCredentials(object):
    """Server credentials"""

    @property
    def ServerName(self):
        return self.server_name

    @property
    def Login(self):
        return self.login

    @property
    def Password(self):
        return self.password

    def __init__(self, server_name, login, password):
       self.server_name = server_name
       self.login = login
       self.password = password

