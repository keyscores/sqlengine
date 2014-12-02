import sys
import urllib
import subprocess
import os

import time

def main(args):
    if len(args) not in (1, 2):
        print 'Pass the full path to Google AppEngine SDK on the command line'
        return

    gae_path = args[0]
    if not os.path.isdir(gae_path):
        print gae_path, 'is not a directory!'
        print
        print 'Pass the full path to Google AppEngine SDK on the command line'
        return

    if len(args) == 2:
        filter_param = '?filter=' + urllib.quote(args[1])
    else:
        filter_param = ''

    try:
        if os.environ.get('KS_DB'):
            with open('ks_db_name.txt', 'w') as f:
                print>>f, os.environ.get('KS_DB')

        pipe = subprocess.Popen([os.path.join(gae_path, 'dev_appserver.py'), os.getcwd(), '--skip_sdk_update_check', 'true'])

        u = None
        for retry in range(20):
            try:
                u = urllib.urlopen('http://localhost:8080/tester' + filter_param)
                break
            except IOError:
                pass
            time.sleep(1)

        if u is None:
            print 'Failed to read test response'
            sys.exit(1)
            results = ''
        else:
            results = u.read()
            print results

    finally:
        if os.path.isfile('ks_db_name.txt'):
            os.remove('ks_db_name.txt')
        pipe.terminate()

    if not results or results.rstrip().splitlines()[-1].upper() != 'OVERALL:OK':
        sys.exit(1)



if __name__ == '__main__':
    main(sys.argv[1:])
