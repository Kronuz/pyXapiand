#!/usr/bin/env python
"""

Start the Xapiand server.

"""
from __future__ import absolute_import, unicode_literals

import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', '..'))

from optparse import make_option, OptionParser

from xapiand import version
from xapiand.server import xapiand_run
from xapiand.platforms import EX_FAILURE, detached

help = "Starts a Xapiand server"
args = ''

base_option_list = (
    make_option('-v', '--verbosity', action='store', dest='verbosity', default='1',
        type='choice', choices=['0', '1', '2', '3', 'v'],
        help="Verbosity level; 0=minimal output, 1=normal output, 2=verbose output, 3=very verbose output"),
    make_option('--pythonpath',
        help="A directory to add to the Python path, e.g. '/home/myproject'."),
    make_option('--traceback', action='store_true',
        help="Print traceback on exception"),
)

option_list = (
    make_option("-D", "--data", action='store', dest='data', default='.'),
    make_option("--logfile", action='store', dest='logfile', default=None),
    make_option("--pidfile", action='store', dest='pidfile', default=None),
    make_option("--uid", action='store', dest='uid', default=None),
    make_option("--gid", action='store', dest='gid', default=None),
    make_option("--umask", action='store', dest='umask', default=0, type='int'),
    make_option("--port", action='store', dest='port', default='0.0.0.0:8890',
        help="Port for the sever, e.g. 0.0.0.0:8890 (default)"),
    make_option("--detach", action='store_true', dest="detach", default=False,
        help="Detach process"),
    make_option("--queue", action='store', dest='queue_type', default='memory',
        type='choice', choices=['memory', 'file', 'redis'],
        help="Queue type; memory=Memory queue (default), file=File based queue (persistent)"),
    make_option("-t", "--commit_timeout", action='store', dest='commit_timeout', default=1, type='int'),
    make_option("--commit_slots", action='store', dest='commit_slots', default=None, type='int'),
)


def detach(path, argv, logfile=None, pidfile=None,
           uid=None, gid=None, umask=0, working_directory=None, fake=False, **options):
    with detached(logfile, pidfile, uid, gid, umask, working_directory, fake):
        try:
            args = list(argv)
            if logfile:
                args.append('--logfile=%s' % logfile)
            if pidfile:
                args.append('--pidfile=%s' % pidfile)
            os.execv(path, args)
        except Exception:
            print >>sys.stderr, "Can't exec %r" % ' '.join([path] + args)
        return EX_FAILURE


def run(logfile=None, pidfile=None, *argv, **options):
    _detach = options.pop('detach', False)
    if _detach:
        logfile = logfile or 'xapiand.log'
        pidfile = pidfile or 'xapiand.pid'
        sys.exit(detach(sys.argv[0], argv, logfile=logfile, pidfile=pidfile, **options))
    else:
        sys.exit(xapiand_run(logfile=logfile, pidfile=pidfile, **options))


def main():
    usage = 'usage: %%prog [options] %s' % args
    if help:
        usage = '%s\n\n%s' % (usage, help)
    parser = OptionParser(usage,
                          version=version,
                          option_list=base_option_list + option_list)

    options, _args = parser.parse_args(sys.argv[1:])
    if options.pythonpath:
        sys.path.insert(0, options.pythonpath)

    run(*_args, **options.__dict__)

if __name__ == '__main__':
    main()
