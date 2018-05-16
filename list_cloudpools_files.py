# Query Platform API to list files that have been CloudPool'd
# joshua.lay@dell.com
#
# Tested on OneFS 8.0.0.4 with default python libraries
# Last update 15-May-2018


from optparse import OptionParser
import getpass
import httplib
import csv
import sys
import socket
import base64
import json


parser = OptionParser()
parser.add_option('-u', '--user', help='required, user account used to query the Platform API', dest='USER')
parser.add_option('-i', '--ip', help='IP address used to query the Platform API', dest='IP', default=socket.gethostbyname(socket.gethostname()))
parser.add_option('-p', '--port', help='TCP port used in URL to query the API, defaults to 8080', dest='PORT', default=8080)
parser.add_option('--csv', help='write output in CSV format, default is list of files', dest='CSV', action="store_true", default=False)
parser.add_option('-n', '--no-header', help='disable header when printing CSV', dest="NO_HEADER", action='store_true', default=False)
parser.add_option('-s', '--show-count', help='print sum of files in jobs-files jobs from the Platform API, this over-rides printing of file names', dest='SHOW_COUNT', action='store_true',  default=False)
opts, args = parser.parse_args()


if (opts.USER == None):
    parser.print_help()
    sys.exit("Username Required")


PASSWORD = getpass.getpass('Enter password: ')

if (PASSWORD == ''):
    sys.exit("Password Required")


HEADERS = {
    'Authorization': "Basic " + base64.b64encode(opts.USER + ':' + PASSWORD),
    'Cache-Control': "no-cache"
}


def get_jobs(ip=opts.IP, port=opts.PORT):
    ''' Get all CloudPool jobs, return list of jobs with ID's greater than 4 (system jobs) '''
    conn = httplib.HTTPSConnection(ip, port)
    conn.request('GET', '/platform/3/cloud/jobs', headers=HEADERS)
    response = conn.getresponse()
    if (response.status != 200):
        print("Response Status: " + str(response.status))
        print("Response Reason: " + str(response.reason))
        sys.exit('HTTP GET Failed on /platform/3/cloud/jobs')
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
    if (response.status != 200):
        print("Response Status: " + str(response.status))
        print("Response Reason: " + str(response.reason))        
        sys.exit('HTTP GET Failed on /platform/3/cloud/jobs-files/<id>')
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
    print
    
    for job in jobs:
        #print(type(job))
        files = job['jobs-files']['files']    # list of dicts
        if (len(files) > 0):
            for file in files:
                print(file['name'])
    print # adds extra newline at end of output


def print_csv(jobs):
    ''' Print all files in jobs-files, output in CSV format (table) '''
    print

    if (opts.NO_HEADER == False):
        print('%s,%s,%s,%s,%s,%s' % ('file_name', 'job_engine_job_id', 'cloudpools_job_id', 'completion_time', 'create_time', 'state_change_time'))
    
    for job in jobs:
        #print(type(job))
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
    print # adds extra newline at end of output
            
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
    
    
