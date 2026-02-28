from setuptools import Extension, setup
from Cython.Build import cythonize

from os.path import abspath, dirname, join
import platform

project_dir = dirname(dirname(abspath(__file__)))
extra_compile_args = [
    "-DCYTHON_LIMITED_API",
    "-DPy_LIMITED_API=0x03080000",
]

if platform.processor() == 'arm':
    extra_compile_args.append('-mcpu=apple-m1')

extensions = [
    Extension(
        "cython_ydb_extension",
        ["cython_ydb_extension.c"],
        include_dirs=[join(project_dir, "c_interface")],
        runtime_library_dirs=[join(project_dir, "go", "_obj")],
        libraries=["ydb"],
        language="c",
        extra_link_args=[
            "-L" + join(project_dir, "go", "_obj"), # library directory path
            "-lydb",
            ],
        extra_compile_args=extra_compile_args,
    )
]

setup(
    options={
        'bdist_wheel': {'universal': False},
    },
    ext_modules=cythonize(extensions),
    packages=['myLib'],
    package_data={'myLib': ['../go/_obj/libydb.so']},
)
