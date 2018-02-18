# import IPython
# IPython.start_ipython(argv=[])

from IPython import get_ipython
ipython = get_ipython()

ipython.magic("load_ext memory_profiler")

import numpy as np

def func(x):
    y = x**2.0
    z = np.exp(y)
    return z

A = np.random.randn(1000, 1000)
ipython.magic("mprun -f func func(A)")