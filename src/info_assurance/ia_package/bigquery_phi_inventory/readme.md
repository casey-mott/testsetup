# BigQuery PHI Inventory

## Purpose

This script identifies where PHI is stored in plain text in Oscar's BigQuery
environment.

There are policies this script enforces:
> Least privilege
> Security safeguards protect sensitive data

## Prerequisites

Before this script can be executed, you must download the team service account
BQ token from our Security LastPass. The location of this JSON file will replace
the null string in line 1 of the config file.

You may need to install dependency libraries.
