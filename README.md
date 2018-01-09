# stellar-utils [![Build Status](https://travis-ci.org/data61/stellar-utils.svg?branch=master)](https://travis-ci.org/data61/stellar-utils)

Utility tools for the Stellar project. This library provides a common interface for the different modules to read/write graphs represented in the EPGM format. It can currently be imported from the maven repository to read/write to/from JSON and Parquet files. 

## Add stellar-utils as a dependency in Maven
Current latest version under development:
```xml
<dependencies>
    <dependency>
        <groupId>sh.serene</groupId>
        <artifactId>stellar-utils</artifactId>
        <version>0.2.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

## Reference
### StellarBackEndFactory
- Currently implemented with `LocalBackEndFactory` and `SparkBackEndFactory` (requires a SparkSession).
- Used to create `StellarGraphMemory` and `StellarReader` objects
### StellarReader
- Used to read EPGM from file
### StellarGraphCollection
- EPGM graph collection
### StellarGraph
- A single graph with supported graph operations
### StellarGraphMemory
- A collection of graph elements

## Basic usage
### Reading a graph from json with given `path` and `graphId`
```java
StellarBackEndFactory backEndFactory = new LocalBackEndFactory(sparkSession);
StellarGraphCollection graphCollection = backEndFactory.reader().format("json").getGraphCollection(path);
StellarGraph graph = graphCollection.get(graphId);
```
### Getting a list of vertices/edges from `graph`
```java
List<Vertex> vertices = graph.getVertices().asList();
List<Edge> edges = graph.getEdges().asList();
```
### Adding a list of edges `newEdges` to `graph`
```java
StellarGraph graphNew = graph.unionEdges(backEndFactory.createMemory(newEdges));
```
### merging `graphNew` into `graphCollection` and writing the result in json
```java
graphCollection.union(graphNew).write().format("json").save(path);
```

## Other examples
Examples can be found [here](src/main/java/sh/serene/stellarutils/examples)

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
