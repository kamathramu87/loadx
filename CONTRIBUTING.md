# How to develop on this project

## Publishing releases

Update the version in `pyproject.toml` before releasing; PyPI versions cannot be
reused. Run `make check` and `make test` before publishing.

The GitHub publishing workflow runs when a GitHub release is published, or when
manually dispatched. It builds the package with `uv build` and uploads through
PyPI Trusted Publishing using the `pypi` GitHub environment.

To build and validate distributions locally without uploading:

```sh
make build-check
```

This replaces `dist/`, builds the wheel and source distribution, and runs
`twine check --strict` using Twine 7 through an isolated uv tool environment. Twine checks
package metadata and the README rendering; it does not test upload credentials
or whether the version is available on PyPI.

For a local upload, supply a PyPI API token through the exported
`UV_PUBLISH_TOKEN` environment variable, then run:

```sh
make publish
```

`make publish` rebuilds and validates before calling `uv publish`. It uploads to
PyPI by default. `make build-and-publish` is an alias for the same sequence.
The token is read directly by uv; do not commit it or put it in the Makefile.
The old `PYPI_TOKEN` variable is no longer used.
