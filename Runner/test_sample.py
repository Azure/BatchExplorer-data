# content of test_sample.py
def func(x):
    return x + 1

def test_answer1():
    assert func(7) == 8

def test_answer2():	
    assert func(4) == 5
    assert func(10) == 11

def test_answer2(): 
    assert func(4) == 6