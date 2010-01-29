import sys

import nose


if __name__ == '__main__':
    args = sys.argv + (
        r'-m (?:^|[\b_\./-])(?:[Tt]est|When|should)'.split())
    nose.run(argv=args)
