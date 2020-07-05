class NotTradable(Exception):
    def __init__(self, error_info):
        super(NotTradable, self).__init__()
        self.error_info = error_info

    def __str__(self):
        return self.error_info

