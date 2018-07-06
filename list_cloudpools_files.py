# Query Platform API to list files that have been CloudPool'd
# This is meant to be run from a cluster node directly and not remotely
#
# Tested on OneFS 8.0.0.4 with default Python 2.6.1 libraries
#
# joshua.lay@dell.com
# Last update 17-May-2018


from optparse import OptionParser
import getpass
import httplib
import sys
import socket
import base64
import json


usage = "Usage: %prog --user USER [options]"
parser = OptionParser(usage=usage, version='%prog 0.4')
parser.add_option('-u', '--user', help='User name used to query the Platform API, if not provided as argument you will be prompted to enter user name', dest='USER')
parser.add_option('-p', '--password', help='Password for user name, if not provided as argument you will be prompted to enter password', dest='PASSWORD')
parser.add_option('-i', '--ip', help='IP address used to query the Platform API, if not provided script attempts to automatically detect local cluster IP or name', dest='IP', default=socket.gethostbyname(socket.gethostname()))
parser.add_option('-P', '--port', help='TCP port used to query the Platform API, defaults to %default', type='int', dest='PORT', default=8080)
parser.add_option('--csv', help='Print output in CSV format, default is list of files', dest='CSV', action="store_true", default=False)
parser.add_option('--no-header', help='Disable header when printing CSV', dest="NO_HEADER", action='store_true', default=False)
parser.add_option('--show-count', help='Print sum of total file counts in jobs-files from the Platform API, this overrides printing of files', dest='SHOW_COUNT', action='store_true',  default=False)
opts, args = parser.parse_args()


if (opts.USER == None):
    opts.USER = raw_input("Enter user: ")

if (opts.USER == ''):    
    parser.print_help()
    sys.exit("User name Required")


if (opts.PASSWORD == None):
    opts.PASSWORD = getpass.getpass('Enter password for %s: ' % (opts.USER))

if (opts.PASSWORD == ''):
    parser.print_help()
    sys.exit("Password Required")


HEADERS = {
    'Authorization': "Basic " + base64.b64encode(opts.USER + ':' + opts.PASSWORD),
    'Cache-Control': "no-cache"
}


def get_jobs(ip=opts.IP, port=opts.PORT):
    ''' Get all CloudPool jobs, return list of jobs with ID's greater than 4 (system jobs) '''
    conn = httplib.HTTPSConnection(ip, port)
    conn.request('GET', '/platform/3/cloud/jobs', headers=HEADERS)
    response = conn.getresponse()
    if (response.status == 401):
        print('401: HTTP Authorization Failed')
        sys.exit(1)    
    elif (response.status != 200):
        print('HTTP GET Failed on /platform/3/cloud/jobs')
        print("Response Status: " + str(response.status))
        print("Response Reason: " + str(response.reason))
        sys.exit(1)
    else:
        #print(response.status)
        data = response.read()
        data = json.loads(data)

    jobs = []

    for job in data['jobs']:
        if (job['id'] > 4):
            jobs.append(job)
    
    return jobs


def add_jobs_files(job, ip=opts.IP, port=opts.PORT):
    ''' Get CloudPool jobs-files based on job ID, append and return jobs-files object to job object '''
    conn = httplib.HTTPSConnection(ip, port)
    conn.request('GET', '/platform/3/cloud/jobs-files/' + str(job['id']), headers=HEADERS)
    response = conn.getresponse()
    if (response.status == 401):
        print('401: HTTP Authorization Failed')
        sys.exit(1)     
    if (response.status != 200):
        print('HTTP GET Failed on /platform/3/cloud/jobs')
        print("Response Status: " + str(response.status))
        print("Response Reason: " + str(response.reason))
        sys.exit(1)
    else:
        #print(response.reason)
        data = response.read()
        data = json.loads(data)    

    job['jobs-files'] = data
    
    # debug stuff
    # print(job['id'])
    # print(job['files']['total'])
    # print('===============================')
    return job


def print_filenames(jobs):
    ''' Print all files in jobs-files, one per line'''
    for job in jobs:
        #print(type(job))
        files = job['jobs-files']['files']    # list of dicts
        if (len(files) > 0):
            for file in files:
                print(file['name'])
        else:
            print('No CloudPools Jobs Files Found\n')
            sys.exit(0)


def print_csv(jobs):
    ''' Print all files in jobs-files, output in CSV format (table) '''
    if (opts.NO_HEADER == False):
        print('%s,%s,%s,%s,%s,%s' % ('file_name', 'job_engine_job_id', 'cloudpools_job_id', 'completion_time', 'create_time', 'state_change_time'))
    
    for job in jobs:
        files = job['jobs-files']['files']    # list of dicts
        if (len(files) > 0):
            for file in files:
                file_name = file['name']
                job_engine_job_id = job['job_engine_job']['id']
                cloudpools_job_id = job['id']
                completion_time = job['completion_time']
                create_time = job['create_time']
                state_change_time = job['state_change_time']
                # file_name, job_engine_job_id, cloudpools_job_id, completion_time, create_time, state_change_time
                print('%s,%s,%s,%s,%s,%s' % (file_name, job_engine_job_id, cloudpools_job_id, completion_time, create_time, state_change_time))
        else:
            print('No CloudPools Jobs Files Found\n')
            sys.exit(0)
   
            
def print_count(jobs):
    ''' Print sum of CloudPool file counts from jobs-file API '''
    count = 0
    for job in jobs:
        total = job['files']['total']
        count += total

    print('Total number of CloudPools files: %s' % (count))
        

def main():
    #print("executing main")

    jobs_list = get_jobs()

    for job in jobs_list:
        job = add_jobs_files(job)
    

    if (opts.SHOW_COUNT == True):
        print_count(jobs_list)
    elif (opts.CSV == True):
        print_csv(jobs_list)
    else:
        print_filenames(jobs_list)
    

if __name__ == "__main__":
    main()
