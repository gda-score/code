# code

This repo holds a variety of tools in support of the GDA Score Project (General Data Anonymity Score Project).

The code here is still very much alpha, and little effort has gone into making it easy for others to install and use.

The primary language is Python, and requires Python3.7 or later. API documentation for some of the tools can be found at https://gda-score.github.io/.

## To run

 - using `pip`:
    - step 1: `$ pip install gda-score-code`
        
    - step 2: initialize the program by executing `$ gdascore_init` in the console right after installation.

 - cloning the library:

This repo has executable .py files in a variety of locations, most notably ./attacks and ./utility.

There are libraries and configuration files located under ./common.

To run these executables, the environment variable `PYTHONPATH` must be set to the path to the repo. (For example, `PYTHONPATH=C:\Users\francis\Documents\GitHub\code;`)


## Repo Organization

This repo is organized as follows:

### anon-methods

Contains tools to implement new anonymization methods.

### attacks

Contains all of the attacks on the various anonymization methods.

### common

Contains general support code.

### graphs

Contains tools to generate diagrams of the attack and utility scores.

### utility

Contains tools to measure the utility of various anonymization methods
