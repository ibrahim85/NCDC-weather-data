import os, sys, numpy, base64, zlib, pickle
import pandas as pd

def loads(eVal):
    """ Decode a string into a value """
    return pickle.loads(zlib.decompress(base64.b64decode(eVal)))
