from time import time
from jeanny3 import *

from .unittests import runtest 

def test_diff_arbitrary():
    raise NotImplementedError

def test_diff_collection():
    raise NotImplementedError

TEST_CASES = [
    test_diff_arbitrary,
    test_diff_collection
]

def do_tests(TEST_CASES,testgroup=None,session_name=None): # test all functions    

    if testgroup is None:
        testgroup = __file__

    session_uuid = uuid()
    
    for test_fun in TEST_CASES:        
        runtest(test_fun,testgroup,session_name,session_uuid,save=True)
        
if __name__=='__main__':
    
    try:
        session_name = sys.argv[1]
    except IndexError:
        session_name = '__not_supplied__'
        
    do_tests(TEST_CASES,session_name=session_name)
    
