import sys
import urllib
import subprocess
import os

import time

def main(args):
    if len(args) != 1:
        print 'Pass the full path to Google AppEngine SDK on the command line'
        return

    gae_path = args[0]
    if not os.path.isdir(gae_path):
        print gae_path, 'is not a directory!'
        print
        print 'Pass the full path to Google AppEngine SDK on the command line'
        return

    pipe = subprocess.Popen([os.path.join(gae_path, 'dev_appserver.py'), os.getcwd()])

    for retry in range(20):
        try:
            u = urllib.urlopen('http://localhost:8080/tester')
            break
        except IOError:
            pass
        time.sleep(1)

    print u.read()
    pipe.terminate()


if __name__ == '__main__':
    main(sys.argv[1:])
