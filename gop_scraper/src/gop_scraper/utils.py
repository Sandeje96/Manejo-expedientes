
import os, time

def timestamp():
    return time.strftime("%Y%m%d-%H%M%S")

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)
    return path
