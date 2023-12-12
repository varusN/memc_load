#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import multiprocessing as mp

from optparse import OptionParser
from functools import partial

# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache

NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

config = {
    "NORMAL_ERR_RATE": 0.01,
    "MEMC_SOCKET_TIMOUT": 3,
    "MEMC_RETRY": 2,
    "CHUNK": 1000,
}

def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(memc, memc_addr, chunk, dry_run=False):
    mset = {}
    for app in chunk:
        ua = appsinstalled_pb2.UserApps()
        ua.lat = app.lat
        ua.lon = app.lon
        key = "%s:%s" % (app.dev_type, app.dev_id)
        ua.apps.extend(app.apps)
        packed = ua.SerializeToString()
        mset[key] = packed
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            for _ in range(config["MEMC_RETRY"]):
                result = memc.set_multi(mset)
                if len(result) > 0:
                    mset = {k:v for k, v in mset.items() if k in result}
            return len(result)
    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
    return config["CHUNK"]


def parse_appsinstalled(line):
    line_parts = line.strip().split("\t")

    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)

def process_file(options, fn):
    logging = logger(options)
    device_memc = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }
    mc = {
        "idfa": [],
        "gaid": [],
        "adid": [],
        "dvid": []
    }
    memc = dict()
    for device_type in device_memc:
        memc[device_type] = memcache.Client([device_memc[device_type]], socket_timeout=config["MEMC_SOCKET_TIMOUT"])
    processed = errors = 0
    logging.info('Processing %s' % fn)
    with gzip.open(fn) as fd:
        for line in fd:
            line = line.decode("UTF-8").strip()
            if not line:
                continue
            appsinstalled = parse_appsinstalled(line)
            if not appsinstalled:
                errors += 1
                continue
            memc_addr = device_memc.get(appsinstalled.dev_type)
            if not memc_addr:
                errors += 1
                logging.error(f'Unknow device type: {appsinstalled.dev_type}, file {fn}')
                continue
            mc[appsinstalled.dev_type].append(appsinstalled)
            if len(mc[appsinstalled.dev_type]) == config["CHUNK"]:
                result = insert_appsinstalled(memc[appsinstalled.dev_type], memc_addr, mc[appsinstalled.dev_type], options.dry)
                mc[appsinstalled.dev_type] = []
                if result > 0:
                    processed += (config["CHUNK"] - result)
                else:
                    errors += result
    for chunk in mc:
        if len(mc[chunk]) > 0:
            result = insert_appsinstalled(memc[chunk], memc_addr, mc[chunk], options.dry)
            if result > 0:
                processed += (config["CHUNK"] - result)
            else:
                errors += result
    if not processed:
        dot_rename(fn)
        return [fn, 0]

    err_rate = float(errors) / processed
    if err_rate < config["NORMAL_ERR_RATE"]:
        logging.info(f'Acceptable error rate ({err_rate}). Successfull loaded file {fn}')
    else:
        logging.error(f'High error rate ({err_rate} > {config["NORMAL_ERR_RATE"]}). Failed to load file {fn}')
        return [fn, 0]
    dot_rename(fn)
    return [fn, processed]

def main(options):
    files = sorted(glob.iglob(options.pattern))
    pool = mp.Pool()
    job = partial(process_file, options)
    for processed_file in pool.imap(job, files):
        if processed_file[1] > 0:
            logging.info(f'File {processed_file[0]} is ready')

def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked

def logger(opts):
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    return logging

if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="/data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging = logger(opts)

    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
