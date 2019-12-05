#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

"""
This is the daemon module and supports all the ReST actions for the
KARIZ cache
"""

# System modules
from datetime import datetime

import cache as ds  # distributed cache
from flask import make_response, abort
import colorama
from colorama import Fore, Style

# 3rd party modules
g_cache = None

def start_cache():
    global g_cache
    cache = ds.Cache(550)
    g_cache = cache
    return cache

def statistics():
    return 0

def cache_status():
    return 0

def cache_mrd(data):
    return {'cached': g_cache.cache_mrd(data['data'])}

def prefetch_mrd(data):
    return {'cached': g_cache.prefetch_mrd(data)}

def cache_plan(data):
    res = g_cache.cache_plan(data['data'], data['score'])
    status = 'cached' if res == 0 else 'is not satisfied'
    print(Fore.MAGENTA, '\n Kariz: cache request for ', str(data), status, str(g_cache), Style.RESET_ALL)
    print(Style.RESET_ALL)
    return {'cached': res}

def prefetch_plan(data):
    res = g_cache.prefetch_plan(data['data'], data['score'])
    status = 'prefetched' if res == 0 else 'is not satisfied'
    print(Fore.GREEN, '\n Kariz: prefetch request: ', str(data), status, str(g_cache), Style.RESET_ALL)
    return {'cached': res}

def is_plancached(data):
    res = g_cache.is_plancached(data['data'])
    status = 'is in cache' if res == 1 else 'is not cached'
    print(Fore.BLUE, '\n Kariz: plan ', str(data['data']), status, str(g_cache), Style.RESET_ALL)
    return {'cached': res}

def cache_file(file):
    return {'cached': g_cache.cache_file(file)}

def unpin_files(plan):
    print(Fore.YELLOW, '\n Kariz: unpin plan ', str(plan), str(g_cache), Style.RESET_ALL)
    return {'cached': g_cache.unpin_files(plan)}

def pin_file(stats):
    return 0

def evict_file(new_stage):
    return 0

def clear_cache():
    print(Fore.RED, '\n clear_cache done: status: ', str(g_cache), Style.RESET_ALL)
    return {'cached': g_cache.clear_cache()}
