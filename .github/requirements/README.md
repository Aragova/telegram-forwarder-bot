# CI dependency locks

These files pin the exact Python packages already exercised by GitHub CI, with SHA-256 digests of the corresponding PyPI artifacts. They target GitHub's Linux x86_64 runner and Python 3.11 (tests/PostgreSQL) or 3.12 (audit). Production requirements and deployment are unchanged.

- `build.lock.txt`: wheel and setuptools build tools, installed first.
- `tests.lock.txt`: runtime dependencies and pytest for Python 3.11.
- `postgres.lock.txt`: additional integration-test dependencies.
- `audit.lock.txt`: static analysis tools for Python 3.12.

Only pyaes and code2flow require source distributions; their hashes are fixed and builds use the separately locked build tools with build isolation disabled. All other Python packages use verified wheels. When requirements or tools change, regenerate the corresponding locks for the CI runner, review package changes and hashes, and run the complete CI suite. Do not remove hash enforcement to work around a mismatch.
