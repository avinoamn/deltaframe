# DeltaFrame

DeltaFrame is a Scala library for efficient snapshot-based delta detection in Spark DataFrames, with support for Apache Iceberg as a snapshot store. It enables users to track changes (new, deleted, and updated records) between DataFrame versions, making it ideal for data pipelines, CDC (Change Data Capture), and data lake architectures.

## Features

- **Delta Detection:** Find new, deleted, and updated rows between DataFrame snapshots.
- **Pluggable Snapshot Store:** Abstracts storage of snapshots; includes an Iceberg-based implementation.
- **Batch & Simple Modes:** Supports both direct comparison and managed batch processing.
- **Spark Integration:** Built on top of Apache Spark DataFrame APIs.
- **Type-Safe Column References:** Uses `scala-nameof` for column safety.
- **Examples Included:** Usage examples and test data for quick onboarding.

## Getting Started

### Prerequisites

- Scala 2.12
- Spark 3.5
- Apache Iceberg
- Maven

### Installation

DeltaFrame is organized as a multi-module Maven project:

- `core`: Core delta detection logic
- `examples`: Example usage and snapshot store implementation

Add the following dependency to your project (see `pom.xml` for details):

```xml
<dependency>
    <groupId>github.avinoamn.deltaframe</groupId>
    <artifactId>deltaframe-core_2.12</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Clone and Build

To clone and build the project locally:

```bash
git clone https://github.com/avinoamn/deltaframe.git
cd deltaframe

# Build with Maven
mvn clean install

# Or build a specific module, e.g. core
mvn -pl core clean install

# Run examples (from root)
mvn -pl examples exec:java -Dexec.mainClass="github.avinoamn.deltaframe.examples.DeltaFrameBatchExample"
```

This will compile all modules and run tests. The built artifacts will be available in the respective `target` directories.

## Usage

### Core API

```scala
import github.avinoamn.deltaframe.DeltaFrame

val deltaframe = DeltaFrame(idColName)
val delta = deltaframe.run(newSnapshotDF, oldSnapshotDF)

delta.newDelta.show()
delta.deletedDelta.show()
delta.updatedDelta.show()
```

### Batch Processing with Iceberg

```scala
import github.avinoamn.deltaframe.DeltaFrame
import github.avinoamn.deltaframe.examples.snapshotStore.EntitiesSnapshotStore

val entitiesSnapshotStore = EntitiesSnapshotStore(...)

val batch = DeltaFrame(idColName).Batch(entitiesSnapshotStore)
val delta = batch.run(newSnapshotDF)
```

See [`examples/DeltaFrameBatchExample.scala`](examples/src/main/scala/github/avinoamn/deltaframe/examples/DeltaFrameBatchExample.scala) for full code.

## Example

The example module provides a complete setup with:

- Entity model and test data
- Iceberg-backed snapshot store
- Example scripts for both direct and batch delta detection

Run with:

```bash
sbt "runMain github.avinoamn.deltaframe.examples.DeltaFrameBatchExample"
```

or with Maven:

```bash
mvn compile exec:java -Dexec.mainClass="github.avinoamn.deltaframe.examples.DeltaFrameBatchExample"
```

## Contributing

Contributions are welcome! Please open issues or PRs for bug fixes, features, or documentation improvements.

## License

MIT License (see [`LICENSE`](LICENSE))

## Project Structure

```
core/
  src/main/scala/github/avinoamn/deltaframe/...
examples/
  src/main/scala/github/avinoamn/deltaframe/examples/...
pom.xml
LICENSE
```

## Acknowledgements

- [Apache Spark](https://spark.apache.org/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [scala-nameof](https://github.com/dwickern/scala-nameof)
