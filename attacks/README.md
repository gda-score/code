# attacks

Contains libraries and attacks for generating GDA Scores (General Data Anonyization Score). The intent is to make it relatively easy for people to develop new attacks.

# Code Documentation

See https://gda-score.github.io/ for code documentation.

# To Use

This folder contains attacks as well as examples of how to design an attack. Best to just take one of the existing attacks as a template and work from there.  The `examples` folder contains additional snippits of code showing how to use different aspects of the attack API (class `gdaAttack()`, class `gdaScores()` etc.)

To run these executables, the environment variable `PYTHONPATH` must be set to the path to the repo. (For example, `PYTHONPATH=C:\Users\francis\Documents\GitHub\code;`)

# Main components

## ../common/config

Note that the config file myCredentials.json must be configured here. (See example template.)

## ..

Contains the attacks themselves. Typically each attack consists of the basic attack code (python) and the configuration for the attack (corresponding .json with the same file name).

## attacks/examples

Contain random examples of how to use the APIs from `../common`.

## attacks/results

Contains the outputs for a number of previously run attacks.

