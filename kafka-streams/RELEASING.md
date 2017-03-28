# Release checklist

The following (perhaps not fully complete) checklist is helpful when performing a "release", which in this case means
a new branch for a specific Confluent release line (e.g. a `3.2.x` branch for the Confluent 3.2 release line).

- Create a release branch, if needed.  Example: `3.2.x` for the Confluent 3.2.x release line.
- Update `pom.xml`, notably `<version>`, `<kafka.version>`, `<confluent.version>`.
- For all instructions in e.g. `README.md` and Javadocs, remove `-SNAPSHOT` from the name of the packaged jar:

        # Snapshot = before release
        $ java -cp target/streams-examples-3.2.0-SNAPSHOT-standalone.jar

        # After release
        $ java -cp target/streams-examples-3.2.0-standalone.jar

- Update, if needed, any references in the instructions to "blobs" or links that are branch-based or tag-based.

        # Such links in the Javadocs would need updating (note the `3.2.x` token)
        <a href='https://github.com/confluentinc/examples/tree/3.2.x/kafka-streams#packaging-and-running'>Packaging</a>

- `README.md`: Update the version compatibility matrix by (1) adding a new entry for the new release and (2) updating
  the entry for the `master` branch.  Pay special attention to the version identifier of the Apache Kafka version.
- Run sth like `git grep 3.2` (here: when releasing Confluent 3.2) to spot any references to the specific release
  version, and update the locations where applicable.

As a follow-up step, you should also:

- Update the `master` branch to track the next line of development, typically based on snapshots/development versions.
