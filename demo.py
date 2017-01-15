import json, requests, datetime, ConfigParser, argparse, sys, csv, yaml, os

from requests.auth import HTTPBasicAuth


#############
# Constants #
#############

MODE_URL = 'https://modeanalytics.com'
CONFIG_FILE = 'mode.yml'


####################
# Helper Functions #
####################

def get_api_url(organisation, report_token):
    return '/api/' + organisation + '/reports/' + report_token

def get_auth(whichAuth, token, password):
    auth = (token, password)
    return auth

def get_response_json(url, auth):
    response = requests.get(url, auth=auth)
    return response.json()


#################
# Main Function #
#################

def main(args):
    # Read command line arguments
    output_directory = args.output_dir

    # Read config file
    with open(CONFIG_FILE, 'r') as f:
        config = yaml.load(f)
    org_name = config['organisation']['name']
    reports  = config['organisation']['reports']
    token    = config['mode']['token']
    password = config['mode']['password']

    report_directory = os.path.join(output_directory, 'mode_reports')
    if not os.path.isdir(report_directory):
        os.makedirs(report_directory)

    if not reports:
        print("No reports listed in {}, exiting".format(
            CONFIG_FILE
        ))
    for reporttoken in reports:
        # Assemble request information
        auth    = get_auth('mode', token, password)
        api_url = get_api_url(org_name, reporttoken)
        url     = MODE_URL + api_url
        print("Dumping queries from report @ {}".format(url))

        # Retrieve and parse data
        data  = get_response_json(url, auth)
        links = data['_links']
        last_run = links['last_successful_run']
        run_url = last_run['href']
        url = MODE_URL + run_url + '/query_runs/'

        data = get_response_json(url, auth)
        embedded = data['_embedded']
        query_runs = embedded['query_runs']

        # Write data to output directory
        query_directory = os.path.join(report_directory, reporttoken, 'queries')
        if not os.path.isdir(query_directory):
            os.makedirs(query_directory)

        files_written = 0
        for query_run in query_runs:
            output_file = os.path.join(
                query_directory,
                'mode_query_{}.sql'.format(query_run['query_token'])
            )
            with open(output_file, 'w') as output:
                output.write(query_run['raw_source'])
            files_written += 1

        print("Wrote {} files to {}".format(
            files_written, query_directory
        ))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-o', '--output-dir', default='./')

    args = parser.parse_args()

    main(args)
