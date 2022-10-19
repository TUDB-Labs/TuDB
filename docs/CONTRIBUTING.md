# Contributing Guide

Welcome to TuDB's contributing guide!

## Local Setup

### Prerequisites

Please install the following before walking through the rest of this guide:

* Java 8
* Maven
* Python (required by pre-commit checks)

### Maven Configuration

If you are a developer in China, please modify your Maven configurations in `~/.m2/settings.xml` to add the Aliyun Maven Central mirror:

```xml
<settings>
    <mirrors>
        <mirror>
            <id>alimaven</id>
            <mirrorOf>central</mirrorOf>
            <name>aliyun maven</name>
            <url>https://maven.aliyun.com/repository/public/</url>
        </mirror>
    </mirrors>
</settings>
```

## Testing

Run the following command to run the test suite: `mvn -B clean install test --file pom.xml`.

### Pre-commit Checks

We run several checks before every commit automatically with `pre-commit`. Please install Python (required by `pre-commit`)
if it's not pre-installed by your operating system. Then install [pre-commit](https://pre-commit.com/) to run
the required checks when you commit your changes.

Once it's installed, run `pre-commit install` to install the hooks that will be run automatically when you `git commit`
your changes. You can also run it via `pre-commit run` on your changes or `pre-commit run --all` to run the checks on
all files.

If you'd like to uninstall the pre-commit hooks, run `pre-commit uninstall`.


## Submit Changes

To submit a change, please follow the following steps:
1. Create a [fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/about-forks) and push changes to a branch in your fork.
1. Create a [pull request from the branch in your fork](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).


## Code Style

1. For Python code, We follows [PEP 8](http://legacy.python.org/dev/peps/pep-0008/) with one exception: lines can be up to 100 characters in length, not 79.
2. For Java code, We follows [Oracle’s Java code](http://www.oracle.com/technetwork/java/codeconvtoc-136057.html) conventions and Scala guidelines below. The latter is preferred.
3. For Scala code, We follows the official [Scala style guide](http://docs.scala-lang.org/style/) and [Databricks Scala guide](https://github.com/databricks/scala-style-guide). The latter is preferred. To format Scala code, run ./dev/scalafmt prior to submitting a PR.
