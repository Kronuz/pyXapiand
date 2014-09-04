from __future__ import absolute_import

__all__ = ['main']


def main():
    from xapiand.bin.worker import main
    main()


if __name__ == '__main__':  # pragma: no cover
    main()
