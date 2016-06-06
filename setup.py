#!/usr/bin/env python
# encoding: utf-8

'''
Usage:
  setup.py <module> --dir=INSTALL_DIR ] [--autostart] [--upgrade|--rollback]
  setup.py (--version|--help)

Arguments:

  module: mw_pusher/mw_repusher/all

Options:

  -h --help                 show this help message and exit
  --version                 show version and exit
  -d --dir INSTALL_DIR      set the install dir
  -a --autostart            the module autostart after setup it
  -u --upgrade              Upgrade the program
  -r --rollback             Rollback the program
'''
import os
import sys
import commands
from docopt import docopt


def get_version(install_dir):
    version = commands.getoutput("cat PROG_VERSION.def | cut -f2 -d=").strip()
    if not version:
        version="bak"
    return version



def main():
    args = docopt(__doc__, version="v0.1")
    #print args
    #return
    '''
    {'--autostart': False,
    '--config': None,
    '--dir': './',
    '--help': False,
    '--version': False,
    '<module>': 'repusher'}
    '''
    module = args['<module>']
    autostart = args['--autostart']
    install_dir = args['--dir']

    if module not in ("mw_pusher", "mw_repusher", "all"):
        exit('<module> must be in ("mw_pusher", "mw_repusher", "all")')

    # copy the module to install_dir

    if args["--upgrade"]:
        # upgrade
        last_version_file = os.path.join(install_dir, "LAST_VERSION")
        if not os.path.exists(install_dir) or os.path.exists(last_version_file):
            print "No old version or upgraded!"
            pass
        else:
            # stop the server
            ret, out = commands.getstatusoutput("supervisorctl -c {} shutdown".format(os.path.join(install_dir,
                "etc", "supervisor.conf")))
            print ret, out
            version = get_version(install_dir)
            backup = os.path.realpath(install_dir.rstrip("/") + '_' + version)
            os.rename(install_dir, backup)
            os.makedirs(install_dir)
            os.system("echo {} > {}".format(backup, last_version_file))
            ret, out = commands.getstatusoutput("cp -R {} {}".format("./", install_dir))
            if ret:
                print >> sys.stderr, "Copy the module files to install dir Failed!"
                exit(out)
            print "Upgrade OK!"

    elif args["--rollback"]:
        # rollback
        # read backup version
        last_version = os.path.join(install_dir, "LAST_VERSION")
        if not os.path.exists(last_version):
            pass
        else:
            # stop the server
            ret, out = commands.getstatusoutput("supervisorctl -c {} shutdown".format(os.path.join(install_dir,
                "etc", "supervisor.conf")))
            print ret, out
            backup = file(last_version).read().strip()
            import shutil
            shutil.rmtree(install_dir)
            os.rename(backup, install_dir)
            print "Rollback OK!"

    else:
        ret, out = commands.getstatusoutput("cp -R {} {}".format("./", install_dir))
        if ret:
            print >> sys.stderr, "Copy the module files to install dir Failed!"
            exit(out)

    # start supervisord using etc/supervisor.conf
    log_dir = os.path.join(install_dir, "var", "log")
    run_dir = os.path.join(install_dir, "var", "run")
    etc_file = os.path.join(install_dir, "etc", "supervisor.conf")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if not os.path.exists(run_dir):
        os.makedirs(run_dir)
    ret, out = commands.getstatusoutput("supervisord -c {}".format(etc_file))
    if ret and "Error: Another program is already listening on a port that one of our HTTP servers is configured to use." not in out:
        print >> sys.stderr, "Run supervisord Failed"
        exit(out)

    if autostart:
        # TODO: supervisorctl start the module
        ret, out = commands.getstatusoutput("supervisorctl -c {} restart {}".format(etc_file, module))
        if "ERROR" in out:
            print >> sys.stderr, "start {} Failed".format(module)
            exit(out)

if __name__ == "__main__":
    main()

