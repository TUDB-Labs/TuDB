# Contributing Guide

Welcome to TuDB's contributing guide!

## Local Setup

### Prerequisites

Please install the following before walking through the rest of this guide:

* Java 8
* Maven

### Maven Configuration

Modify your Maven configurations in `~/.m2/settings.xml` to add the private servers and mirrors that this project
depends on:

```xml
<settings>
    <servers>
        <server>
            <id>maven-public</id>
            <username>admin</username>
            <password>qohpof-vigkag-5hastA</password>
        </server>
        <server>
            <id>maven-snapshot</id>
            <username>admin</username>
            <password>qohpof-vigkag-5hastA</password>
        </server>
    </servers>
    <mirrors>
        <mirror>
            <id>alimaven</id>
            <mirrorOf>central</mirrorOf>
            <name>aliyun maven</name>
            <url>https://maven.aliyun.com/repository/public/</url>
        </mirror>
        <mirror>
            <id>maven-public</id>
            <mirrorOf>*</mirrorOf>
            <name>pandadb public</name>
            <url>http://123.57.165.135:30100/repository/maven-public</url>
        </mirror>
        <mirror>
            <id>maven-snapshot</id>
            <mirrorOf>*</mirrorOf>
            <name>pandadb snapshot</name>
            <url>http://123.57.165.135:30100/repository/maven-snapshots</url>
        </mirror>
    </mirrors>
</settings>
```

## Testing

Run the following command to run the test suite: `mvn -B clean install test --file pom.xml`.

## Code Review Guidelines

Before considering how to contribute code, it’s useful to understand how code is reviewed, and why changes may be rejected.
See the detailed guide for code reviewers from [Google’s Engineering Practices documentation](https://google.github.io/eng-practices/review/).
Simply put, changes that have many or large positives, and few negative effects or risks, are much more likely to be merged, and merged quickly.
Risky and less valuable changes are very unlikely to be merged, and may be rejected outright rather than receive iterations of review.

Positives
- Fixes the root cause of a bug in existing functionality
- Adds functionality or fixes a problem needed by a large number of users
- Simple, targeted
- Maintains or improves consistency across Python, Java, Scala
- Easily tested; has tests
- Reduces complexity and lines of code
- Change has already been discussed and is known to committers

Negatives, risks
- Band-aids a symptom of a bug only
- Introduces complex new functionality, especially an API that needs to be supported
- Adds complexity that only helps a niche use case
- Adds user-space functionality that does not need to be maintained in Database,
- Changes a public API or semantics (rarely allowed)
- Adds large dependencies
- Changes versions of existing dependencies
- Adds a large amount of code
- Makes lots of modifications in one “big bang” change


## How to send a push request

### Create a GitHub issue at first
Generally, We use GitHub issue to track logical issues, including bugs and improvements,
and uses GitHub pull requests to manage the review and merge of specific code changes.
That is, GitHub issue are used to describe what should be fixed or changed, and high-level approaches,
and pull requests describe how to implement that change in the project’s source code.
For example, major design decisions are discussed in GitHub issue.

If this PR is required, create a new GitHub issue:
1. Provide a descriptive Title. “Update web UI” or “Problem in scheduler” is not sufficient. “Kafka Streaming support fails to handle empty queue in YARN cluster mode” is good.
2. Write a detailed Description. For bug reports, this should ideally include a short reproduction of the problem. For new features, it may include a design document.
3. Set required fields:
- Issue Type. Generally, Bug, Improvement and New Feature are the only types used in Spark.
- Priority. Set to Major or below; higher priorities are generally reserved for committers to set. The main exception is correctness or data-loss issues, which can be flagged as Blockers. JIRA tends to unfortunately conflate “size” and “importance” in its Priority field values. Their meaning is roughly:
- Blocker: pointless to release without this change as the release would be unusable to a large minority of users. Correctness and data loss issues should be considered Blockers for their target versions.
- Critical: a large minority of users are missing important functionality without this, and/or a workaround is difficult
- Major: a small minority of users are missing important functionality without this, and there is a workaround
- Minor: a niche use case is missing some support, but it does not affect usage or is easily worked around
- Trivial: a nice-to-have change but unlikely to be any problem in practice otherwise
  Component
- Affects Version. For Bugs, assign at least one version that is known to exhibit the problem or need the change

###  Push request

Before creating a pull request, it is important to check if tests can pass on your branch because our GitHub Actions workflows automatically run tests for your pull request/following commits
and every run burdens the limited resources of GitHub Actions in the upstream repository.
Below steps will take your through the process.

1. Fork the GitHub repository at https://github.com/TUDB-Labs/TuDB-Embedded if you haven’t already
2. Go to “Actions” tab on your forked repository and enable “Build and test” and “Report test results” workflows
3. Clone your fork and create a new branch
4. Consider whether documentation or tests need to be added or updated as part of the change, and add them as needed.
- When you add tests, make sure the tests are self-descriptive.
- Also, you should consider writing a Github Issue ID in the tests when your pull request targets to fix a specific issue.
    - Scala
      ```scala
      test("TuDB-12345: a short description of the test") {
        ...
      ```
    - Python
  ```python
  def test_case(self):
      # issue-12345: a short description of the test
      ...
  ```
5. Consider whether benchmark results should be added or updated as part of the change, and add them as needed by Running benchmarks in your forked repository to generate benchmark results.
6. Run all tests with ./dev/run-tests to verify that the code still compiles, passes tests, and passes style checks. If style checks fail, review the Code Style Guide below.
7. Push commits to your branch. This will trigger “Build and test” and “Report test results” workflows on your forked repository and start testing and validating your changes.
8. Open a pull request against the master branch of usptream. (Only in special cases would the PR be opened against other branches). This will trigger workflows “On pull request*” (on upstream repo) that will look/watch for successful workflow runs on “your” forked repository (it will wait if one is running).
    - The PR title should be of the form [TuDB-xxxx][COMPONENT] Title, where TuDB-xxxx is the relevant github number, COMPONENT is one of the PR categories and Title may be the title or a more specific title describing the PR itself.
    - If the pull request is still a work in progress, and so is not ready to be merged, but needs to be pushed to GitHub to facilitate review, then add [WIP] after the component.
    - Consider identifying committers or other contributors who have worked on the code being changed. Find the file(s) in GitHub and click “Blame” to see a line-by-line annotation of who changed the code last. You can add @username in the PR description to ping them immediately.
    - Please state that the contribution is your original work and that you license the work to the project under the project’s open source license.
9. Watch for the results, and investigate and fix failures promptly
    - Fixes can simply be pushed to the same branch from which you opened your pull request
    - Github will automatically re-test when new commits are pushed

### Review process
1. Other reviewers, including committers, may comment on the changes and suggest modifications. Changes can be added by simply pushing more commits to the same branch
2. Sometimes, other changes will be merged which conflict with your pull request’s changes. The PR can’t be merged until the conflict is resolved. This can be resolved by, for example, adding a remote to keep up with upstream changes by
   "git remote add upstream git@github.com:TUDB-Labs/TuDB-Embedded.git", running
   git fetch upstream followed by "git rebase upstream/master" and
   resolving the conflicts by hand, then pushing the result to your branch.

###  Code style guide
1. For Python code, We follows [PEP 8](http://legacy.python.org/dev/peps/pep-0008/) with one exception: lines can be up to 100 characters in length, not 79.
2. For Java code, We follows [Oracle’s Java code](http://www.oracle.com/technetwork/java/codeconvtoc-136057.html) conventions and Scala guidelines below. The latter is preferred.
3. For Scala code, We follows the official [Scala style guide](http://docs.scala-lang.org/style/) and [Databricks Scala guide](https://github.com/databricks/scala-style-guide). The latter is preferred. To format Scala code, run ./dev/scalafmt prior to submitting a PR.

Note: This document mainly follow the rule of [Spark community](https://spark.apache.org/contributing.html), Thanks for the useful guideline. 