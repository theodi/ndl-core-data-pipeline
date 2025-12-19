"""Run all unit tests under the `tests/` directory using Python's unittest discovery.

This script discovers tests with the pattern `test_*.py` starting at the directory
where this file lives (`tests/`) and runs them with verbosity. It exits with
non-zero status if any tests fail (suitable for CI).
"""
import os
import sys
import unittest


def main(start_dir: str = None, pattern: str = 'test_*.py') -> int:
    if start_dir is None:
        start_dir = os.path.abspath(os.path.dirname(__file__))

    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=start_dir, pattern=pattern)

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Return exit code 0 if all tests passed, 1 otherwise
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    rc = main()
    sys.exit(rc)

