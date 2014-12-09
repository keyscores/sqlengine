import sys
import urllib
import subprocess
import os

import time

def main(args):
    if '-h' in args or '--help' in 'args' or len(args) == 0:
        print 'Usage:'
        print 'python gaetestrunner.py path-to-GAE-SDK [+|-test-filter] [db-env-name]'
        print
        print 'Filters must be preceded by a "+" (positive filter, "run only this test")'
        print 'or a "-", negative filter "run all but this test"'
        print 'currently, only one filter is supported'
        return

    gae_path = args[0]
    if not os.path.isdir(gae_path):
        print gae_path, 'is not a directory!'
        print
        print 'Pass the full path to Google AppEngine SDK on the command line'
        return

    filter_param = ''
    db_name = ''
    if len(args) > 1:
        for a in args[1:]:
            if a[0] == '+':
                filter_param = '?filter=' + urllib.quote(a[1:])
            elif a[0] == '-':
                filter_param = '?filter=' + urllib.quote(a)
            else:
                db_name = a


    pipe = None
    try:
        if db_name:
            with open('ks_db_name.txt', 'w') as f:
                print>>f, db_name

        print 'db_name is', db_name or '(default)'
        print 'filter:', filter_param or '(None)'

        pipe = subprocess.Popen([os.path.join(gae_path, 'dev_appserver.py'), os.getcwd(), '--skip_sdk_update_check', 'true'])

        url = 'http://localhost:8080/tester' + filter_param
        print 'running tests at URL', url
        u = None
        for retry in range(20):
            try:
                u = urllib.urlopen(url)
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
        if pipe:
            pipe.terminate()

    if not results or results.rstrip().splitlines()[-1].upper() != 'OVERALL:OK':
        sys.exit(1)



if __name__ == '__main__':
    main(sys.argv[1:])
