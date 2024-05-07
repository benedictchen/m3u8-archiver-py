""" Download all .ts file parts from a m3u8 file, given a url."""
import json
import sys
import os
import requests
import re
import argparse
from google.oauth2 import service_account
from google.cloud import storage


MAX_WORKERS = 10
OUTPUT_FOLDER = 'output'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:57.0)"
    " Gecko/20100101 Firefox/57.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache"
}

def downloadM3u8(m3u8url,
                 headers=HEADERS,
                 depth=0,
                 gcp_bucket=None,
                 gcp_project=None,
                 gcp_folder=None):
    """ recursively download m3u8 files"""
    storage_client = None
    if gcp_bucket is not None:
        storage_client = storage.Client(project=gcp_project)

    if not os.path.isdir(OUTPUT_FOLDER):
        os.mkdir(OUTPUT_FOLDER)
    base_url = '/'.join(m3u8url.split('/')[0:-1]) + '/' # get the base url
    print('processing: {}'.format(m3u8url))
    m3u8_payload = requests.get(m3u8url, headers=HEADERS) # get the m3u8 file
    folder = m3u8url.split('/')[-2] # get the filename
    parent_folder = None
    if depth > 0:
        parent_folder = m3u8url.split('/')[-3]
    m3u8_filename = m3u8url.split('/')[-1].split('?')[0] # get the filename


    local_path = getCleanPath(OUTPUT_FOLDER, parent_folder, folder)
    gcp_path = getCleanPath(gcp_folder, parent_folder, folder)

    local_target_path = os.path.join(local_path, m3u8_filename)

    if not os.path.isdir(local_path):
        os.mkdir(local_path)
    with open(local_target_path, 'wb') as f:
        print('writing file to {}'.format(local_target_path))
        f.write(m3u8_payload.content)

    if gcp_bucket is not None:
        gcs_src = os.path.join(local_path, m3u8_filename)
        gcs_target = os.path.join(gcp_path, m3u8_filename)
        print('uploading to GCS: ', gcs_src, gcs_target)
        uploadToGCS(storage_client,
                    gcp_bucket,
                    gcs_src,
                    gcs_target)

    # Download encrypted key files
    key_urls = extractKeyUrls(m3u8_payload)
    keys_to_upload = []

    for key_url in key_urls:
        key_filename = key_url.split('/')[-1].split('?')[0]
        key_file = requests.get(base_url + key_url, headers=HEADERS)
        with open(os.path.join(local_path, key_filename), 'wb') as f:
            f.write(key_file.content)
        keys_to_upload.append(getCleanPath(parent_folder, folder, key_filename))

    print('keys_to_upload', keys_to_upload)

    if gcp_bucket is not None:
        for key_to_upload in keys_to_upload:
            uploadToGCS(storage_client,
                        gcp_bucket,
                        getCleanPath(OUTPUT_FOLDER, key_to_upload),
                        getCleanPath(gcp_folder, key_to_upload))

    ts_urls = extractTsUrls(m3u8_payload) # get all the .ts urls from m3u8 file.

    # find all the .ts files in the local directory so we can exclude them.
    ts_files = set(filter(lambda x: '.ts' in x, os.listdir(local_path)[0:-1]))

    print('Found existing ts files: {} total'.format(len(ts_files)))
    if len(ts_files) > 0:
        ts_urls = list(filter(lambda x: x.split('?')[0] not in ts_files, ts_urls))

    # Download the files locally.
    for ts in ts_urls:
        ts_url = base_url + ts
        print('downloading: {}'.format(ts_url))
        ts_filename = ts.split('?')[0]
        ts_file = requests.get(ts_url, headers=HEADERS)
        with open(os.path.join(local_path, ts_filename), 'wb') as f:
            f.write(ts_file.content)

    # Upload everything to GCP.
    if gcp_bucket is not None:
        # list all ts files again.
        ts_files_done = set(filter(lambda x: '.ts' in x, os.listdir(local_path)))
        # ts_files_to_upload = []
        for ts_file in ts_files_done:
            ts_filename = ts_file.split('/')[-1].split('?')[0]
            ts_file_src = getCleanPath(local_path, ts_file)
            ts_file_dest = os.path.join(gcp_path, ts_filename)
            # ts_files_to_upload.append((ts_file_src, ts_file_dest))
            uploadToGCS(storage_client,
                        gcp_bucket,
                        ts_file_src,
                        ts_file_dest)


    child_urls = extractM3u8Urls(m3u8_payload) # get all the urls in the m3u8 file
    all_urls = []
    for child in child_urls:
        new_url = base_url + child
        all_urls.append(new_url)
        subchildren = downloadM3u8(new_url,
                                   headers=HEADERS, depth=depth + 1,
                                   gcp_bucket=gcp_bucket,
                                   gcp_project=gcp_project,
                                   gcp_folder=gcp_folder)
        all_urls.extend(subchildren)
    return all_urls

def getCleanPath(*args):
    result = list(filter(lambda x : x is not None,
                                    list(args)))
    return os.path.join(*result)

def extractTsUrls(m3):
    """ get a list of .ts urls from the m3u8 file """
    lines = m3.text.split('\n')
    urls = []
    for line in lines:
        if '.ts' in line:
            urls.append(line)
    return urls

def extractM3u8Urls(m3):
    """ get a list of m3u8 urls from the m3u8 file """
    lines = m3.text.split('\n')
    urls = []
    for line in lines:
        if '.m3u8' in line:
            urls.append(line)
    return urls

def extractKeyUrls(m3):
  """ get a list of key urls from the m3u8 file """
  lines = m3.text.split('\n')
  urls = []
  for line in lines:
    match = re.search(r'URI="([^"]+)"', line)
    if match:
      urls.append(match.group(1))
  return urls

def uploadToGCS(storage_client, bucket_name,
                source, destination):
    """ upload a file to a google cloud storage bucket """
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination)
    blob.upload_from_filename(source)
    print('[UPLOAD] -> File {} uploaded to {}'.format(source, destination))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcp_bucket', help='Google Cloud Storage bucket name')
    parser.add_argument('--gcp_project', help='Google Cloud Storage project name')
    parser.add_argument('--gcp_folder', help='Google Cloud Storage dest folder name')
    parser.add_argument('--gcp_creds_json', help='Google Cloud Storage credentials json file')
    parser.add_argument('m3u8_url', nargs='+', help='m3u8 url root file')
    args = parser.parse_args()
    print('GCP project: ', args.gcp_project)
    print('GCP bucket: ', args.gcp_bucket)
    print('m3u8 root url: ', args.m3u8_url[0])
    print('GCP creds json: ', args.gcp_creds_json)

    gcp_credentials = None

    if args.gcp_bucket is not None:
        assert args.gcp_project is not None, 'GCP project must be provided'
        assert args.gcp_creds_json is not None, 'GCP creds json must be provided'
        assert args.gcp_folder is not None, 'GCP folder must be provided'

    # FIXME(benedictchen): Is there a better way to pass credentials to GCP client?
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.gcp_creds_json

    all_m3u8 = downloadM3u8(args.m3u8_url[0], headers=HEADERS,
                            gcp_bucket=args.gcp_bucket,
                            gcp_project=args.gcp_project,
                            gcp_folder=args.gcp_folder)

    print('DONE!')
