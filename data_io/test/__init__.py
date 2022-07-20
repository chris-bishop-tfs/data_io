from test_connection import Data_ioConnectioTest

def check_connection():
    result = Data_ioConnectioTest().execute_tests()
    print(result.to_string())  