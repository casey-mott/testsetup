# BigQuery User Access

## Purpose

This script parses through the IAM policies for BigQuery projects. The IAM policy
binds together principals and roles, which governs access.

There are policies this script enforces:
> Terminated employees do not have any access
> Access is not assigned to any individual principal

## Prerequisites

Before this script can be executed, you must download the team service account
BQ token from our Security LastPass. The location of this JSON file will replace
the null string in line 1 of the config file. 

You may need to install dependency libraries.
