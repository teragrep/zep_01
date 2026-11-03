# Overview
Regex interpreter for Teragrep

# Implementation
Takes a regex on the first line, rest is payload, uses reflection to print out the named match group keys and their values.

# Usage

```
%regex
^(?<line>.*)$
this line will be capture into line


