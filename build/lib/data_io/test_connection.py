from runtime.nutterfixture import NutterFixture, tag
from data_io import build_connection

''' we will be using nutter since unittest does not 
work in data
'''

class data_io_connection_test(NutterFixture):

    '''
    where going to have the user be able to 
    put the connection in
    '''

    def __init__(self, orracle_url=None, s3_url=None, redshift_url= None):
        self.orracle_url = orracle_url
        self.orracle_check = orracle_url.split('@')[1]
        self.redshift_url = redshift_url
        self.redshift_check = 'jdbc:redshift://' + redshift_url.split('@')[1]
        # our check for s3 connection should just include the url
        self.s3_url = s3_url

        NutterFixture.__init__(self)

    # testing orracle connection
    def assertion_orracle_test(self):
        connection = build_connection(self.orracle_url)
        check = connection.jdbc_url.split('//')[1]
        assert(check == self.orracle_check)

    # testing redshift connection
    def assertion_redshift_test(self):
        connection = build_connection(self.redshift_url)
        test = connection.jdbc_url
        assert(test == self.redshift_check)

    # s3 connection test
    def assertion_s3_test(self):
        connection = build_connection(self.s3_url)
        assert(connection.url == self.s3_url)

# calling connection test
def connection_test(orracle_url=None, s3_url=None, redshift_url= None):
    return data_io_connection_test(orracle_url, s3_url, redshift_url).execute_tests()


