#!/usr/bin/env python
# -- coding: utf-8 --
import argparse
import ConfigParser
import logging
import os
import subprocess
import shlex
from argparse import ArgumentParser
from datetime import datetime

Config = ConfigParser.ConfigParser()
#logging.basicConfig(filename=os.path.basename("C:\\Users\\HoneyS\\Desktop\\a.txt"),
                    #filemode='a',
                    #format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    #datefmt='%H:%M:%S',
                    #level=logging.DEBUG)


def run_win_cmd(cmd):
    result = []
    logging.info("Running command {}".format(cmd))
    process = subprocess.Popen(shlex.split(cmd),
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    for line in process.stdout:
        result.append(line)
    errcode = process.returncode
    for line in result:
        print(line)
    if errcode is not None:
        raise Exception('cmd %s failed, see above for details', cmd)


def download_views(args):

    # Parsing details from config file
    Config.read(args.tableau_config)
    server = Config.get('Tableau', 'server')
    site = Config.get('Tableau', 'site')
    user = Config.get('Tableau', 'user')
    password = Config.get('Tableau', 'password')
    path = Config.get('PathName', 'path')
    Config.read(args.datefilter_config)
    date = Config.get('DateFilter', 'date')
    

    logging.info("Logging in")
    cmd = "tabcmd login -s {} -t {} -u {} -p {}".format(
        server,
        site,
        user,
        password
    )
    run_win_cmd(cmd)
    with open(args.view_config, mode="r+") as f:
        lines = f.readlines()
        for line in lines:
            dashboard, view, save_as, *unused= [item.strip() for item in line.split(',', 2)]
            download_path = "{}\\\{}.csv".format(path, save_as)
            logging.info("Downloading {} and saving as {} for date {}".format(
                dashboard, view, download_path, date))
            cmd = "tabcmd export {}/{}?Date={} --csv -f {}".format(
                dashboard, view, date, download_path)
            run_win_cmd(cmd)


if __name__ == '__main__':
    parser = ArgumentParser(prog=__file__)
    parser.add_argument(
        '--tableau_config',
        required=True,
        help='Tableau server details and other details'
    )
    parser.add_argument(
        '--datefilter_config',
        required=True,
        help='date filter value'
    )
    parser.add_argument(
        '--view_config',
        required=True,
        help='File containing list of paths and name to save as'
    )

    args = parser.parse_args()
    download_views(args)
	
# Usage: C:\Users\Raghav\Desktop\tableau-download-util>download_util.py --tableau_config login_path_config.ini --datefilter_config datefilter.ini --view_config axiom_views_config.txt
	
	
