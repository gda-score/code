# attacks

Contains libraries and attacks for generating GDA Scores (General Data Anonyization Score). The intent is to make it relatively easy for people to develop new attacks.

# Code Documentation

See https://gda-score.github.io/ for code documentation.

# To Use

This folder contains attacks as well as examples of how to design an attack. Best to just take one of the existing attacks as a template and work from there.  The `examples` folder contains additional snippits of code showing how to use different aspects of the attack API (class `gdaAttack()`, class `gdaScores()` etc.)

# Main components

## ../common/config

Databases (currently limited to postgres and aircloak) may be configured in a file formated like `../common/config/databases.json`. (Note that the code in `../common` assumes a databases config file called `../common/config/myDatabases.json`.)

## ..

Contains the attacks themselves. Typically each attack consists of the basic attack code (python) and the configuration for the attack (corresponding .json with the same file name).

## attacks/examples

Contain random examples of how to use the APIs from `../common`.

