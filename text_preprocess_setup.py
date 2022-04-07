from setuptools import setup
from Cython.Build import cythonize
import numpy

setup(
    name = "Text Preprocess",
    ext_modules = cythonize("text_preprocess.pyx"),
    include_dirs=[numpy.get_include()]
)
