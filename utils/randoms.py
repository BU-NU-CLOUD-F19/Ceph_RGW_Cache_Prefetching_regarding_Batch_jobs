#!/usr/bin/python3
# Trevor Nogues, Mania Abdi

import random

def input_generator_alph():
    '''
    This function generat a list of inputs
    dataset names are alphabers and their
    sizes is in the range specified in here.
    '''
    global inputs;
    random_inputs = {}
    letter = "a"
    while letter <= "z":
        random_inputs[letter] = random.randint(1,10)
        letter = chr(ord(letter)+1)

    return random_inputs;
