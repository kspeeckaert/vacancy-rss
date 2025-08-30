import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from random import randint
import time
import requests
from requests.exceptions import HTTPError
from feedgen.feed import FeedGenerator

import json
from yattag import Doc

BASE_URL = 'https://editx.eu/en/it-jobs'
BASE_URL_REST = 'https://api.editx.eu/en/api/editx/jobs'
BASE_URL_DETAIL_REST = 'https://api.editx.eu/en/api/editx/job'
FEED_ID = 'd34c4e59-e6f2-4fce-8ea6-e0da265fbf22'


def create_session() -> requests.Session:
    sess = requests.Session()

    sess.headers = {'Content-Type': 'application/json',
               'Accept': 'application/json',
               'Origin': 'https://editx.eu',
               'Referer': 'https://editx.eu/',
               'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15'
              }
    return sess


def get_data(config_file: Path|str) -> list:
    
    logging.debug('Reading configuration file...')
    with open(config_file) as f:
        config_data = json.load(f)

    sess = create_session()
    
    try:
        r = sess.get(BASE_URL_REST, 
                     params=config_data)
        r.raise_for_status()
        raw_data = r.json()
        data = raw_data['jobs']

        if (total := int(raw_data['total'])) > (count := int(raw_data['count'])):
            page_count = total % count
            logging.debug(f'Total count is {total}, need to issue another {page_count-1} requests to fetch all data.')
            # We already had to retrieve page 0 to figure this out,
            # so the iterations need to start from 1
            for i in range(1,page_count):
                logging.debug(f'Requesting data ({i+1}/{page_count})...')
                config_data['page'] = i
                r = sess.get(BASE_URL_REST, 
                            params=config_data)
                r.raise_for_status()
                data.extend(r.json()['jobs'])
    except Exception as e:
        logging.error(f'Failed to retrieve data: {e!r}')
        raise

    logging.debug(f'Data retrieved successfully, {len(data)} records.')
    return data


def get_posting_details(data: list) -> dict:
    
    details = {}
    post_count = len(data)
    sess = create_session()
    
    for i, entry in enumerate(data):
        entry_id = entry['id']
        logging.debug(f'Retrieving details for post ID {entry_id}...')
        if entry_id in details:
            logging.warning(f'Detail data for post ID {entry_id} already exists. This should not happen!')
            continue

        try:
            r = sess.get(f'{BASE_URL_DETAIL_REST}/{entry_id}')
            r.raise_for_status()
            details[entry_id] = r.json()
            if i < post_count-1:
                time.sleep(randint(3,10))
        except Exception as e:
            logging.error(f'Exception while retrieving details for post ID {entry_id}: {e!r}.')
    
    logging.debug(f'Retrieved {len(details)} detail record(s).')
    return details


def generate_feed(data: list, details: dict, output_file: Path|str) -> None:

    def get_native_ts(ts_string):
        try:
            ts = datetime.strptime(ts_string, '%Y-%m-%d %H:%M:%S')
            ts.replace(tzinfo=timezone.utc)
        except Exception as e:
            logging.warning(f'Failed to convert "{ts_string}" to datetime: {e!r}')
            return None

    fg = FeedGenerator()
    run_timestamp = datetime.now(timezone.utc)
    fg.title('editx')
    fg.id(FEED_ID)
    fg.link(href='https://www.editx.eu', rel='alternate')
    fg.lastBuildDate(run_timestamp)
    fg.updated(run_timestamp)
    
    for entry in data:
        fe = fg.add_entry()
        entry_id = entry['id']
        try:
            fe.title(f'{entry['title']} ({entry['company']['label']})')
            link = f'{BASE_URL}/{entry_id}'
            fe.id(link)
            fe.link(href=link)
            fe.published(get_native_ts(entry['onlineDate']['value']))
            fe.updated(get_native_ts(entry['changed']['value']))

            detail_data = details.get(entry_id)

            # Build the entry content
            doc, tag, text, line = Doc().ttl()
            with tag('p'):
                with tag('ul'):
                    line('li', f'Bedrijf: {entry['company']['label']}')
                    line('li', f'Recruiter: {entry['recruiter']['firstname']} {entry['recruiter']['lastname']}, {entry['recruiter']['position']['position']['label']} – {entry['recruiter']['position']['organization']['label']}')
                    line('li', f'Locatie: {entry['locality']} ({entry.get('address')})')
                    line('li', f'Published on {entry['onlineDate']['label']}, last change {entry['changed']['label']}')
            
            if detail_data is not None:
                try:
                    line('h3', 'Rol')
                    doc.asis(detail_data['role'])
                except Exception:
                    pass
                try:
                    line('h3', 'Profiel')
                    doc.asis(detail_data['profile'])
                except Exception:
                    pass
                try:
                    line('h3', 'Aanbod')
                    doc.asis(detail_data['proposal'])
                except Exception:
                    pass
                try:
                    line('h3', 'Bedrijf')
                    with tag('p'):
                        with tag('ul'):
                            line('li', f'Adres: {detail_data['company']['address']}')
                            line('li', f'Vacatures: {detail_data['company']['countJobOnline']}')
                            line('li', f'Werknemers: {detail_data['company']['numberOfEmployees']['label']}')
                            line('li', f'Sector: {detail_data['company']['industry']['label']}')
                except Exception:
                    pass
            
            fe.content(doc.getvalue(), type='html')

            categories = [{'term': skill['label']} for skill in entry['skills']]
            fe.category(categories)
        except Exception as e:
            logging.error(f'Failed to generate entry for post ID {entry_id}: {e!r}')

    logging.info(f'Writing to {output_file}...')
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
    parser = argparse.ArgumentParser(description='Generate RSS feeds for VDAB jobs.')
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
        logging.error(f'File not found: {config_file}')
        exit(1)

    main(config_file=args.config_file, 
         output_file=args.output_file,
         fetch_details=args.details)
