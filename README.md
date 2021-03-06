# stellar-utils [![Build Status](https://travis-ci.org/data61/stellar-utils.svg?branch=master)](https://travis-ci.org/data61/stellar-utils)

Utility tools for the Stellar project. This library provides a common interface for the different modules to read/write graphs represented in the EPGM format. It can currently be imported from the maven repository to read/write to/from JSON and Parquet files. 

### Build
- To run the unit tests: `mvn test`
- To build the jars: `mvn package`

### Add stellar-utils as a dependency in Maven
```xml
<dependencies>
    <dependency>
        <groupId>sh.serene</groupId>
        <artifactId>stellar-utils</artifactId>
        <version>1.0</version>
    </dependency>
</dependencies>
```

## License

Copyright 2017 CSIRO Data61

Licensed under  the Apache License, Version  2.0 (the "License"); you  may not
use  the files  included  in this  repository except  in  compliance with  the
License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless  required  by  applicable  law   or  agreed  to  in  writing,  software
distributed under  the License  is distributed  on an  "AS IS"  BASIS, WITHOUT
WARRANTIES OR  CONDITIONS OF  ANY KIND,  either express  or implied.   See the
License for the specific language  governing permissions and limitations under
the License.
