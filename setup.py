import fnmatch
import setuptools
from setuptools.command.build_py import build_py as build_py_orig

with open("README.md", "r") as fh:
    long_description = fh.read()

# excluded = ['common/gdaTool.py', 'common/gdaQuery.py']
excluded = []


class build_py(build_py_orig):
    def find_package_modules(self, package, package_dir):
        modules = super().find_package_modules(package, package_dir)
        return [
            (pkg, mod, file)
            for (pkg, mod, file) in modules
            if not any(fnmatch.fnmatchcase(file, pat=pattern) for pattern in excluded)
        ]


setuptools.setup(
    name="gda-score-code",
    # name="gda-score-code-test",
    version="2.4.1",
    # version="2.5.9",  # for test.pypi
    author="Paul Francis",
    description="Tools for generating General Data Anonymity Scores (www.gda-score.org)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gda-score/code",
    packages=setuptools.find_packages(include=['gdascore', 'gdascore.global_config']),
    cmdclass={'build_py': build_py},
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    package_data={'': ['master.json', 'myCredentials.json', 'config_var.json']},
    include_package_data=True,
    install_requires=[
        'numpy>=1.16.0',
        'pprint>=0.1',
        'matplotlib>=3.0.2',
        'python-dateutil>=2.7.5',
        'simplejson>=3.16.0',
        'psycopg2>=2.8.3',
        'PyInquirer==1.0.3',
        'pyfiglet==0.8.post1',
        'coloredlogs==14.0'
    ],
    entry_points={
        'console_scripts': [
            'gdascore_init = gdascore.cli:init',
        ]
    }
)
