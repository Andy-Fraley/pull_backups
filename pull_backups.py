#!/usr/bin/env python

import sys
import logging
import re
import boto3
import datetime
import os
import argparse
import urllib
import tempfile
import subprocess
import shutil
from util import util
import json


# Fake class only for purpose of limiting global namespace to the 'g' object
class g:
    program_filename = None 
    websites = None
    message_output_filename = None


def main(argv):

    parser = argparse.ArgumentParser()
    parser.add_argument('--message-output-filename', required=False, help='Filename of message output file. If ' \
        'unspecified, then messages are written to stderr as well as into the ./tmp/messages_[datetime_stamp].log ' \
        'file.')

    g.args = parser.parse_args()

    g.program_filename = os.path.basename(__file__)
    if g.program_filename[-3:] == '.py':
        g.program_filename = g.program_filename[:-3]

    message_level = 'Info'

    if g.args.message_output_filename is not None:
        g.message_output_filename = g.args.message_output_filename

    util.set_logger(message_level, g.message_output_filename, os.path.basename(__file__))

    with open('./pull_backups.config') as config_settings_file:
        config_settings = json.load(config_settings_file)
        temp_dir = tempfile.mkdtemp(prefix='pull_backups_')
        file_dir = temp_dir + '/' + datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        os.mkdir(file_dir)
        for config_set in config_settings['config_sets']:
            for config_set_name in config_set:
                aws_access_config = config_set[config_set_name]['aws_access']
                backup_folders = config_set[config_set_name]['backup_folders']
                for backup_folder in backup_folders:
                    # Find latest backup in 'daily' folder of S3 bucket 'ingomarchurch_website_backups'
                    s3 = boto3.resource('s3', aws_access_key_id=aws_access_config["access_key_id"],
                        aws_secret_access_key=aws_access_config["secret_access_key"],
                        region_name=aws_access_config["region_name"])
                    file_items = [item for item in s3.Bucket(aws_access_config["s3_bucket_name"]).objects.all() \
                        if item.key[-1] != '/']
                    newest_sortable_str = ''
                    obj_to_retrieve = None
                    for file_item in file_items:
                        path_sects = file_item.key.split('/')
                        if backup_folder != '' and len(path_sects) == 3:
                            if path_sects[0] == backup_folder:
                                if path_sects[1] == 'daily':
                                    filename = path_sects[2]
                                    match = re.match('(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})' + \
                                        '(?P<hours>[0-9]{2})(?P<minutes>[0-9]{2})(?P<seconds>[0-9]{2})\.zip', filename)
                                    if match is not None:
                                        sortable_str = match.group('year') + match.group('month') + \
                                            match.group('day') + match.group('hours') + match.group('minutes') + \
                                            match.group('seconds')
                                        if sortable_str > newest_sortable_str:
                                            newest_sortable_str = sortable_str
                                            obj_to_retrieve = file_item
                                    else:
                                        message("Unrecognized file in 'daily' backup folder...ignoring: " + \
                                            file_item.key)
                        elif backup_folder == '' and len(path_sects) == 2:
                            if path_sects[0] == 'daily':
                                filename = path_sects[1]
                                match = re.match('(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})' + \
                                    '(?P<hours>[0-9]{2})(?P<minutes>[0-9]{2})(?P<seconds>[0-9]{2})\.zip', filename)
                                if match is not None:
                                    sortable_str = match.group('year') + match.group('month') + \
                                        match.group('day') + match.group('hours') + match.group('minutes') + \
                                        match.group('seconds')
                                    if sortable_str > newest_sortable_str:
                                        newest_sortable_str = sortable_str
                                        obj_to_retrieve = file_item
                                else:
                                    message("Unrecognized file in 'daily' backup folder...ignoring: " + \
                                        file_item.key)
                    if obj_to_retrieve is not None:
                        # Generate 10-minute download URL
                        s3Client = boto3.client('s3', aws_access_key_id=aws_access_config["access_key_id"],
                            aws_secret_access_key=aws_access_config["secret_access_key"],
                            region_name=aws_access_config["region_name"])
                        url = s3Client.generate_presigned_url('get_object',
                            Params = {
                                'Bucket': aws_access_config["s3_bucket_name"],
                                'Key': obj_to_retrieve.key
                            }, ExpiresIn = 10 * 60)
                        if backup_folder == '':
                            backup_folder = 'ccb'
                        print 'Retrieving: ' + file_dir + '/' + backup_folder + '.zip'
                        urllib.urlretrieve(url, file_dir + '/' + backup_folder + '.zip')
                    else:
                        message('Error finding latest backup file to retrieve. Aborting!')
                        sys.exit(1)

    # Instructions to user on how to rsync to Synology (can be unreliable) and then how to remove temp files
    # on this Mac
    print 'Files are now downloaded into a date-stamped directory inside a temporary directory.'
    print 'Execute the following commands to (1) push backup files to Synology and (2) delete the temporary directory'
    print 'and intermediate files stored on this Mac.'
    print
    print 'rsync --append --rsync-path=/bin/rsync -aviz ' + config_settings["rsync_shell_flags"] + ' "' + \
        file_dir + '" ' + config_settings["rsync_remote_target_dir"]
    print
    print 'rm -rf ' + temp_dir

    # If rsync to Synology ran reliably, these would be commands to automatically rsync and delete the files
    #os.system('rsync --append --rsync-path=/bin/rsync -aviz ' + config_settings["rsync_shell_flags"] + ' "' + \
    #    file_dir + '" ' + config_settings["rsync_remote_target_dir"])
    #shutil.rmtree(temp_dir)
    #message_info('Temporary output directory deleted')

    sys.exit(0)


def message(str):
    global g

    datetime_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #print >> sys.stderr, datetime_stamp + ':' + g.program_filename + ':' + level + ':' + s
    print >> sys.stderr, datetime_stamp + ':' + g.program_filename + ':' + ':' + str


def message_info(s):
    logging.info(s)
    output_message(s, 'INFO')


def message_warning(s):
    logging.warning(s)
    output_message(s, 'WARNING')


def message_error(s):
    logging.error(s)
    output_message(s, 'ERROR')


def output_message(s, level):
    global g

    # Only echo to stderr if logger is logging to file (and not stderr)
    if g.args.message_output_filename is not None:
        datetime_stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print >> sys.stderr, datetime_stamp + ':' + g.program_filename + ':' + level + ':' + s


if __name__ == "__main__":
    main(sys.argv[1:])
