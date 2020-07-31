# NiFi Daffodil Processors

## Overview

This repository contains the source for two NiFi processors which use
[Apache Daffodil (incubating)](https://daffodil.apache.org),
an open source implementation of the [Data Format Description Language
(DFDL)](https://www.ogf.org/ogf/doku.php/standards/dfdl/dfdl) to parse/unparse
data to/from NiFi Records, which is then transformed into an Infoset based on the supplied NiFi Controller.  The two processors included are:

* DaffodilParse: Reads a FlowFile and parses the data into a NiFi Record, which is then converted into an Infoset by a NiFi RecordSetWriter component.
* DaffodilUnparse: Reads a FlowFile containing an infoset in some form, reads it using the correct NiFi RecordReader component and converts it into Records, and then unparses these Records to the original file format.

## Processor Properties

Each Processor has a number of configurable properties intended to be analogous to the [CLI options](https://daffodil.apache.org/cli/) for the Daffodil tool. Here are is a note about the __Stream__ option:
- __Stream Mode:__ This mode is disabled by default, but when enabled parsing will continue even if the end of the input is reached; Parsing will continue until either there an issue was encountered, or the end of the input was reached.  If they are all successful, a Set of Records will be generated.  When using this mode for the XML Reader and Writer components, the Writer component must be configured with a name for the Root Tag, and the Reader component must be configured with "Expect Records as Array" set to true.

## Build Instructions

This repository uses the maven build environment. To create a nar file for use
in Apache NiFi, run

    mvn install

This command will create a nar file in `nifi-daffodil-nar/target/`, which can
be copied to the Apache NiFi lib directory to install into the Apache NiFi
environment.

## License

NiFi Daffodil Processors is licensed under the Apache Software License v2.