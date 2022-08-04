# Information Assurance Python Package

## Purpose

This folder holds:
1. Script code used for version control. For details on scripts, visit script readmes
2. Functions that can be imported as modules and executed with version control

## How To Use package

```
pip install git+ssh://git@stash.hioscar.com:7999/SEC/security-scripts.git#subdirectory=info_assurance
```

Create a py file and import the modules and functions you need, i.e.:

```
from info_assurance.common_functions import read_from_bigquery
```
