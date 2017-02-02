# Daffodil NiFi Processor

## Overview

This repository contains the source for two NiFi processors which use
[Daffodil](https://opensource.ncsa.illinois.edu/confluence/display/DFDL/Daffodil%3A+Open+Source+DFDL),
an open source implementation of the [Data Format Description Language
(DFDL)](https://www.ogf.org/ogf/doku.php/standards/dfdl/dfdl) to parse/unparse
data to/from an XML infoset. The two processor included are:

* DaffodilParse: Reads a FlowFile and parses the data into an XML infoset
* DaffodilUnparse: Reads a FlowFile, in the form of an XML infoset, and
  unaprses the infoset to the original file format

## Build Instructions

This repository uses the maven build environment. To create a nar file for use
in Apache NiFi, run

    mvn install

This command will create a nar file in `nifi-daffodil-nar/target/`, which can
be copied to the Apache NiFi lib directory to install into the Apache NiFi
environment.
