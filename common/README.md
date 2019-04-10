# code/common

Contains general support tools.

### config

Holds the database configuration file. See `config/README.md` for directions.

### examples

Holds examples of some of the tools in this directory. (Other examples can be found in other directories.)

### gdaQuery.py

This is a tool that can be used to generate SQL queries whose answers contain a specified number of distinct users.

### gdaScore.py

This is the primary support tool for attacks. It automatically keeps track of various scores during various phases of an attack (gathering knowledge, making attack queries, and making claims). It provides an asynchronous query interface, and can queue queries for parallel execution. See https://gda-score.github.io/gdaScore.m.html.

### gdaTool.py

This tool makes a variety of basic measure of database columns.

### gdaUtilities.py

A mish mash of basic functions. See https://gda-score.github.io/gdaUtilities.m.html.
