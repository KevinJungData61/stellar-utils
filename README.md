#serene-utils 

Utility tools for the Stellar project. This library provides a common interface for the different modules to read/write graphs represented in the EPGM format. It can currently be imported from the maven repository to read/write to/from JSON and Parquet files. 

###Build
- To run the unit tests: `mvn test`
- To build the jars: `mvn package`

###Add serene-utils as a dependency in Maven
```xml
<dependencies>
    <dependency>
        <groupId>sh.serene</groupId>
        <artifactId>serene-utils</artifactId>
        <version>1.0</version>
    </dependency>
</dependencies>
```

###License
Copyright © 2017 CSIRO Data61 All rights reserved
