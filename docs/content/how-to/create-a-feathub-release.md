# Create a FeatHub Release

## Prepare for Release

### Create a new Milestone in Github

FeatHub uses [Github
Milestones](https://docs.github.com/en/issues/using-labels-and-milestones-to-track-work/about-milestones)
to track the issues to be solved in each release. With the release currently
underway, you should create new MileStones as subsequent releases for new
issues.

1. Navigate to the [Milestone](https://github.com/alibaba/feathub/milestones)
   page of FeatHub's Github Repo, and click the "New milestone" button at the
   top right.
2. Suppose the version to be released is 0.1.0, you should create milestones
   whose names are "release-0.1.1" and "release-0.2.0". The milestones' due date
   and description need not be configured.

### Triage release-blocking Issues in Github

There could be outstanding release-blocking issues, which should be triaged
before proceeding to build a release candidate. Suppose the version to be
released is 0.1.0, there should have been a Milestone named "release-0.1.0"
created on Github, and release-blocking issues have been attached to this
milestone. Triage each unresolved issue in this milestone with one of the
following resolutions:

- If the issue has been resolved but its state was not updated, resolve it
  accordingly.
- If the issue has not been resolved and it is acceptable to defer this until
  the next release, update the issue's milestone attribute to the new version
  you just created. Please consider discussing this with stakeholders as
  appropriate.
- If the issue has not been resolved and it is not acceptable to release until
  it is fixed, the release cannot proceed. Instead, work with the FeatHub
  developers to resolve the issue.

### Create a release branch

Release candidates are built from a release branch. You should create the
release branch, push it to the code repository (you should probably do this once
the whole process is done), and update version information on the original
branch.

Set up the following environment variables to simplify commands that follow. (We
use `bash` Unix syntax in this guide.)

```shell
export RELEASE_VERSION="0.1.0"
export SHORT_RELEASE_VERSION="0.1"
export CURRENT_SNAPSHOT_VERSION="$SHORT_RELEASE_VERSION-SNAPSHOT"
export NEXT_SNAPSHOT_VERSION="0.2-SNAPSHOT"
```

If you are doing a new major/minor release (e.g. 0.1.0, 0.2.0), check out the
version of the codebase from which you start the release. This may be `HEAD` of
the `master` branch. Create a branch for the new version that we want to release
before updating the master branch to the next development version:

```shell
git checkout master
RELEASE_VERSION=$SHORT_RELEASE_VERSION tools/releasing/create_release_branch.sh
git checkout master
OLD_VERSION=$CURRENT_SNAPSHOT_VERSION NEW_VERSION=$NEXT_SNAPSHOT_VERSION tools/releasing/update_branch_version.sh
git checkout release-$SHORT_RELEASE_VERSION
```

If you're creating a new bugfix release (e.g. 0.1.1), you will skip the above
step and simply check out the the already existing branch for that version:

```shell
git checkout release-$SHORT_RELEASE_VERSION
```

If this is a major release, the newly created branch needs to be pushed to the
official repository.

The rest of this guide assumes that commands are run in the root of a repository
on the branch of the release version with the above environment variables set.

### Checklist to proceed to the next step

- All release blocking Github issues have been listed in the corresponding
  Milestone, and have been closed.
- Release branch has been created and pushed if it is a major release.
- Originating branch has the version information updated to the new version.

## Build a Release Candidate

### Build and stage Python artifacts

```shell
OLD_VERSION=$CURRENT_SNAPSHOT_VERSION NEW_VERSION=$RELEASE_VERSION tools/releasing/update_branch_version.sh
tools/releasing/create_python_sdk_release.sh
```

You will be able to find the built source and Python release artifacts under the
"tools/releasing/release/" folder.


### Prepare a Release Note

See previous Release Notes [here](https://github.com/alibaba/feathub/releases)
for how to write a new note for this release. The items listed in the release
note should meet the following criteria:

- Be appropriately classified as `Bug`, `New Feature`, `Improvement`, etc.
- Represent noteworthy user-facing changes, such as new functionality,
  backward-incompatible API changes, or performance improvements.
- Have occurred since the previous release; an issue that was introduced and
  fixed between releases should not appear in the Release Notes.
- Have an issue title that makes sense when read on its own.

Adjust any of the above properties to improve the clarity and presentation of
the Release Notes.

### Stage the artifacts and Publish the Release Note

Set up a few environment variables to identify the release candidate being
built. Start with `RC_NUM` equal to `1` and increment it for each candidate.

```shell
export RC_NUM="1"
export TAG="release-${RELEASE_VERSION}-rc${RC_NUM}"
```

Tag and push the release commit:

```shell
git tag -a ${TAG} -m "${TAG}"
git push <remote> refs/tags/${TAG}
```

Release manager should create a Github token with permissions to create Github
release and upload the artifacts to the Feathub repository. After you have
created and saved the token, create a pre-release and upload the release
artifacts to Github with the following command:

```shell
GITHUB_TOKEN=<YOUR-TOKEN> tools/releasing/create_github_release_and_upload_artifacts.sh
```

Then, head over to the release you just created on GitHub
(https://github.com/alibaba/feathub/releases/${TAG}) and publish the release
note to the description of the release.

### Verify the artifacts

Release manager should guarantee the correctness of the release candidate in the
following aspects:

- The source codes should be able to pass all the FeatHub tests. This should
  have been verified by FeatHub's CI on Github.
- The [FeatHub end-to-end
  examples](https://github.com/flink-extended/feathub-examples) should be able
  to function correctly with the Python artifacts. This could be verified by
  manually triggering the corresponding
  [CI](https://github.com/flink-extended/feathub-examples/actions/workflows/build-and-test.yaml)
  with the release artifact as the Target FeatHub Python package.

### Checklist to proceed to the next step

- The release commit has been tagged and pushed to Github
- The release commit has a corresponding Github Release with all the release
  artifacts
- Release information properly described in the release note written in the
  Github Release.
- The correctness of the source code and Python artifact have been verified and
  the verification result is available to reviewers (e.g. as a Github Action
  link).

## Vote on the release candidate

After the steps above, you should share the release candidiate with the Feathub
repository admins to request their approval on this releast candidate. The
sharing message should contain the following information:

- The link to the Github pre-release you just created, which contains the
  following
  - The release note in the description section.
  - The source code archive in the asset list.
  - The Python artifact with its .sha512 file in the asset list.
- The link to the Github Action in FeatHub's repository that demonstrates the
  correctness of the source code.
- The link to the Github Action in FeatHub's end-to-end examples' repository
  that demonstrates the correctness of the Python artifact.

Feathub repository admins are free to double-verify any of the checklist items
described in sections above before they vote to approve the release candidate.
The release manager and reviewers can (optionally) also do additional
verification by

- Check hashes (e.g. `shasum -c *.sha512`)
- grep for legal headers in each file.

If any issues are identified and should be fixed in this release during the
review, fix codes should be pushed to master and release branch through the
normal contributing process. Then you should go back and build a new release
candidate with these changes.

### Checklist to proceed to the next step

- The Feathub repository admins agree to release the proposed candidate.

## Finalize the Release

### Deploy Python artifacts to Pypi

Release manager should create a PyPI account and ask the Feathub repository
admins to add this account to feathub collaborator list with Maintainer role to
deploy the Python artifacts to PyPI. The artifacts could be uploaded using
twine(https://pypi.org/project/twine/). To install twine, just run:

```shell
pip install --upgrade twine==1.12.0
```

Download the python artifact (`feathub-$RELEASE_VERSION-py3-none-any.whl`) from
Github release manually and upload it to [pypi.org](http://pypi.org/):

```shell
twine upload feathub-$RELEASE_VERSION-py3-none-any.whl
```

### Git Release

Create a new Git Release for the released version by copying the tag for the
final release candidate, as follows:

```shell
git tag -a "release-${RELEASE_VERSION}" refs/tags/${TAG}^{} -m "Release Feathub ${RELEASE_VERSION}"
git push <remote> refs/tags/release-${RELEASE_VERSION}
TAG="release-${RELEASE_VERSION}" GITHUB_TOKEN=<YOUR-TOKEN> tools/releasing/create_github_release_and_upload_artifacts.sh
```

Then, head over to the release you just created on GitHub
(https://github.com/alibaba/feathub/releases/${TAG}), publish the release note
to the description of the release, and unmark it with "pre-release".


### Close milestone for current release

The Github Milestone for the current release should be closed. No new issues
should be linked to this milestone any longer.

### Final checklist

- Python artifacts released and indexed in the
  [PyPI](https://pypi.org/project/feathub/) Repository
- The formal Github Release is created in the source code repository, and its
  description and artifact should be the same as that in the final release
  candidate.
- Milestone for current release closed
