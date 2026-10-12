import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from random import randint
import time
import math
import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from feedgen.feed import FeedGenerator

import json
from yattag import Doc

__version__ = '0.2.20261012'

BASE_URL = 'https://editx.eu/en/it-jobs'
BASE_URL_REST = 'https://api.editx.eu/en/api/editx/jobs'
BASE_URL_DETAIL_REST = 'https://api.editx.eu/en/api/editx/job'
FEED_ID = 'd34c4e59-e6f2-4fce-8ea6-e0da265fbf22'
# (connect, read) timeout in seconds for every HTTP request
REQUEST_TIMEOUT = (10, 60)


def create_session() -> requests.Session:
    sess = requests.Session()

    sess.headers = {'Content-Type': 'application/json',
               'Accept': 'application/json',
               'Origin': 'https://editx.eu',
               'Referer': 'https://editx.eu/',
               'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15'
              }
    # Retry transient failures (connection errors, rate limiting, server errors)
    retry = Retry(total=3, backoff_factor=2,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=None)
    sess.mount('https://', HTTPAdapter(max_retries=retry))
    return sess


def get_data(config_file: Path|str) -> list:
    
    logging.debug('Reading configuration file...')
    with open(config_file) as f:
        config_data = json.load(f)

    sess = create_session()
    
    try:
        r = sess.get(BASE_URL_REST,
                     params=config_data,
                     timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        raw_data = r.json()
        data = raw_data['jobs']
        total = int(raw_data['total'])
        count = int(raw_data['count'])
    except Exception as e:
        logging.error(f'Failed to retrieve data: {e!r}')
        raise

    if count > 0 and total > count:
        page_count = math.ceil(total / count)
        logging.debug(f'Total count is {total}, need to issue another {page_count-1} request(s) to fetch all data.')
        # We already had to retrieve page 0 to figure this out,
        # so the iterations need to start from 1
        for i in range(1,page_count):
            logging.debug(f'Requesting data ({i+1}/{page_count})...')
            config_data['page'] = i
            try:
                r = sess.get(BASE_URL_REST,
                            params=config_data,
                            timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                data.extend(r.json()['jobs'])
            except Exception as e:
                # Keep the pages we already have
                logging.error(f'Failed to retrieve page {i+1}/{page_count}, continuing with {len(data)} records: {e!r}')
                break

    logging.debug(f'Data retrieved successfully, {len(data)} records.')
    return data


def get_posting_details(data: list) -> dict:
    
    details = {}
    post_count = len(data)
    sess = create_session()
    
    for i, entry in enumerate(data):
        entry_id = entry.get('id')
        if entry_id is None:
            logging.warning(f'Post {i} has no ID, skipping details.')
            continue
        logging.debug(f'Retrieving details for post ID {entry_id}...')
        if entry_id in details:
            logging.warning(f'Detail data for post ID {entry_id} already exists. This should not happen!')
            continue

        try:
            r = sess.get(f'{BASE_URL_DETAIL_REST}/{entry_id}', timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            details[entry_id] = r.json()
        except Exception as e:
            logging.error(f'Exception while retrieving details for post ID {entry_id}: {e!r}.')
        # Also wait after a failure, to avoid hammering the server
        if i < post_count-1:
            time.sleep(randint(3,10))
    
    logging.debug(f'Retrieved {len(details)} detail record(s).')
    return details


def collect_items(post_id, items) -> list:
    '''Evaluate each item function and keep the results that do not fail.'''
    result = []
    for item in items:
        try:
            result.append(item())
        except Exception as e:
            logging.debug(f'Post {post_id}: field left out: {e!r}')
    return result


def generate_feed(data: list, details: dict, output_file: Path|str) -> None:

    def get_native_ts(ts_data):
        # editx sends each time as a UNIX timestamp and as a UTC string
        try:
            return datetime.fromtimestamp(int(ts_data['timestamp']), timezone.utc)
        except Exception:
            pass
        try:
            return datetime.strptime(ts_data['value'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        except Exception as e:
            logging.warning(f'Failed to convert {ts_data!r} to datetime: {e!r}')
            return None

    fg = FeedGenerator()
    run_timestamp = datetime.now(timezone.utc)
    fg.title('editx')
    fg.id(FEED_ID)
    fg.link(href='https://www.editx.eu', rel='alternate')
    fg.lastBuildDate(run_timestamp)
    fg.updated(run_timestamp)
    
    for entry in data:
        entry_id = entry.get('id')
        fe = fg.add_entry()
        # Required fields: if one of these fails, only this entry is dropped
        try:
            if entry_id is None:
                raise ValueError('post has no ID')
            company = (entry.get('company') or {}).get('label')
            fe.title(f'{entry['title']} ({company})' if company else entry['title'])
            link = f'{BASE_URL}/{entry_id}'
            fe.id(link)
            fe.link(href=link)
        except Exception as e:
            logging.error(f'Failed to generate entry for post ID {entry_id}, skipping it: {e!r}')
            fg.remove_entry(fe)
            continue

        # Optional fields: a failure only leaves out that part
        fe.published(get_native_ts(entry.get('onlineDate')))
        fe.updated(get_native_ts(entry.get('changed')))

        detail_data = details.get(entry_id)

        # Build the entry content
        try:
            doc, tag, text, line = Doc().ttl()
            summary = collect_items(entry_id, (
                lambda: f'Bedrijf: {entry['company']['label']}',
                lambda: f'Recruiter: {entry['recruiter']['firstname']} {entry['recruiter']['lastname']}, {entry['recruiter']['position']['position']['label']} – {entry['recruiter']['position']['organization']['label']}',
                lambda: f'Locatie: {entry['locality']} ({entry.get('address')})',
                lambda: f'Published on {entry['onlineDate']['label']}, last change {entry['changed']['label']}',
            ))
            if summary:
                with tag('p'):
                    with tag('ul'):
                        for item in summary:
                            line('li', item)

            if isinstance(detail_data, dict):
                for heading, key in (('Rol', 'role'), ('Profiel', 'profile'), ('Aanbod', 'proposal')):
                    html = detail_data.get(key)
                    if isinstance(html, str) and html.strip():
                        line('h3', heading)
                        doc.asis(html)
                company_items = collect_items(entry_id, (
                    lambda: f'Adres: {detail_data['company']['address']}',
                    lambda: f'Vacatures: {detail_data['company']['countJobOnline']}',
                    lambda: f'Werknemers: {detail_data['company']['numberOfEmployees']['label']}',
                    lambda: f'Sector: {detail_data['company']['industry']['label']}',
                ))
                if company_items:
                    line('h3', 'Bedrijf')
                    with tag('p'):
                        with tag('ul'):
                            for item in company_items:
                                line('li', item)

            fe.content(doc.getvalue(), type='html')
        except Exception as e:
            logging.warning(f'Failed to build content for post ID {entry_id}: {e!r}')

        try:
            categories = [{'term': skill['label']} for skill in entry['skills']]
            if categories:
                fe.category(categories)
        except Exception as e:
            logging.debug(f'Post {entry_id}: categories left out: {e!r}')

    # Do not replace a working feed with an empty one
    entry_count = len(fg.entry())
    if entry_count == 0:
        raise RuntimeError('The feed has no entries, the existing feed file is kept.')

    logging.info(f'Writing {entry_count} entries to {output_file}...')
    fg.atom_file(output_file, pretty=True)
    logging.info(f'RSS feed saved.')


def main(config_file: Path|str, output_file: Path|str, fetch_details: bool=False) -> None:
    logging.info('Retrieving data...')
    data = get_data(config_file)
    if fetch_details:
        logging.info('Retrieving posting details...')
        details = get_posting_details(data)
    else:
        details = {}
    logging.info('Building RSS feed...')
    generate_feed(data, details, output_file)
    logging.info('Done!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate RSS feeds for editx jobs.')
    parser.add_argument('output_file', help='Filename to write the RSS feed to')
    parser.add_argument('config_file', help='Filename containing the request configuration')
    parser.add_argument('-d', '--details', action='store_true', help='Enable job details')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()

    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    # Avoid logging from requests
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    # Verify if configuration file exists
    if not Path(args.config_file).is_file():
        logging.error(f'File not found: {args.config_file}')
        sys.exit(1)

    try:
        main(config_file=args.config_file,
             output_file=args.output_file,
             fetch_details=args.details)
    except Exception:
        # A non-zero exit code stops the workflow before it commits the feed
        logging.exception('Feed generation failed.')
        sys.exit(1)
