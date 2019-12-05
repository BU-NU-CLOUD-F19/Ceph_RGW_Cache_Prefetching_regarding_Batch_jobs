#!/usr/bin/python

import utils.randoms as randoms


if _inputs is None:
      inputs = randoms.input_generator_alph();
_inputs = {'a': 2, 'b': 9, 'c': 9, 'd': 9, 'e': 9, 'f': 5, 'g': 2, 'h': 2, 'i': 10, 'j': 2, 'k': 10, 'l': 9, 'm': 5, 'n': 4, 'o': 7, 'p': 6, 'q': 3, 'r': 4, 's': 9, 't': 4, 'u': 1, 'v': 2, 'w': 7, 'x': 10, 'y': 4, 'z': 10}


# FIXME
def test_initialization(self):
    cache_entries = {'f': 5, 't': 4, 'l': 9, 'r': 4, 'g': 2, 'h': 2, 'v': 2, 'o': 7, 'n': 4, 'c': 9, 'd': 9, 'k': 10, 'x': 10, 'z': 10, 'j': 2, 'i': 10 }
    for ce in cache_entries:
        if randLetter not in self.cache:
            added.append(randLetter)
            letterSpace = _inputs[randLetter]
            self.cache.append(CacheEntry(randLetter, letterSpace, 0))
            numAdded += letterSpace
            self.free_space -= letterSpace
            self.used_space = self.size - self.free_space


def random_initialization(self):
    numAdded = 0
    added = []
    '''
    If still possible to fit max letter size
    Currently max is 100, might be changed
    '''
    print(_inputs)
    while self.free_space > 10:
        randLetter = chr(random.randint(97,122))
        if randLetter not in self.cache:
            added.append(randLetter)
            letterSpace = _inputs[randLetter]
            self.cache.append(CacheEntry(randLetter, letterSpace, 0))
            numAdded += letterSpace
            self.free_space -= letterSpace
            self.used_space = self.size - self.free_space
