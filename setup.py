import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="gda-score-code",
    version="2.0.4",
    author="Paul Francis",
    description="Tools for generating General Data Anonymity Scores (www.gda-score.org)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/gda-score/code",
    packages=setuptools.find_packages(where='common', exclude=['gdaQuery', 'gdaTool', 'config', 'examples']),
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    include_package_data=True,

    # py_modules=['gdaScore'],
    # package_dir={'': 'common'},
    install_requires=['numpy>=1.16.0', 'pprint==0.1', 'matplotlib==3.0.2', 'python-dateutil==2.7.5']
)
