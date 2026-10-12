import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from random import randint
import time
import sys
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from feedgen.feed import FeedGenerator

import json
from yattag import Doc

__version__ = '0.2.20261012'

BASE_URL = 'https://www.vdab.be/vindeenjob/vacatures'
BASE_URL_REST = 'https://www.vdab.be/rest/vindeenjob/v4/vacatures'
PAGE_COUNT = 20
FEED_ID = 'e2590d85-368a-4ef9-863c-931baa40fcec'
# (connect, read) timeout in seconds for every HTTP request
REQUEST_TIMEOUT = (10, 60)


def create_session() -> requests.Session:
    sess = requests.Session()

    sess.headers = {'Content-Type': 'application/json',
               'Accept': 'application/json, text/plain, */*',
               'Referer': 'https://www.vdab.be/vindeenjob/vacatures?locatie=2550%20Kontich&afstand=30&locatieCode=375&sort=standaard&diplomaNiveau=D&arbeidscircuit=8&jobdomein=JOBCAT10&arbeidsduur=V&ervaring=4&arbeidsregime=D&taal=N_15',
               'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15',
               'Vej-Key-Monitor': 'b277002f-e1fa-4fc5-868a-fdab633c3851'
              }
    # Retry transient failures (connection errors, rate limiting, server errors).
    # allowed_methods=None also retries the search POST, which does not change data.
    retry = Retry(total=3, backoff_factor=2,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=None)
    sess.mount('https://', HTTPAdapter(max_retries=retry))
    return sess


def get_data(config_file: Path|str, page_count: int) -> list:
    
    logging.debug('Reading configuration file...')
    with open(config_file) as f:
        config_data = json.load(f)

    data = []
    sess = create_session()
    
    try:
        for i in range(page_count):
            logging.debug(f'Requesting data ({i}/{page_count})...')
            config_data['pagina'] = i
            r = sess.post('https://www.vdab.be/rest/vindeenjob/v4/vacatureLight/zoek',
                          data=json.dumps(config_data),
                          timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            partial_data = r.json()['resultaten']
            if len(partial_data) == 0:
                logging.info(f'Bailing out early, no data received for page {i}')
                break
            data.extend(partial_data)
            if i < page_count-1:
                time.sleep(randint(3,10))
    except Exception as e:
        logging.error(f'Exception at attempt {i}: {e!r}. Total entries retrieved: {len(data)}.')
    else:
        logging.debug(f'Data retrieved successfully, {len(data)} records.')
    return data


def get_entry_id(entry: dict):
    '''Return the VDAB post ID, or None when the post has no usable ID.'''
    try:
        return entry['id']['id']
    except (KeyError, TypeError):
        return None


def collect_items(post_id, items) -> list:
    '''Evaluate each item function and keep the results that do not fail.'''
    result = []
    for item in items:
        try:
            result.append(item())
        except Exception as e:
            logging.debug(f'Post {post_id}: field left out: {e!r}')
    return result


def get_posting_details(data: list) -> dict:
    
    details = {}
    post_count = len(data)
    sess = create_session()
    
    for i, entry in enumerate(data):
        entry_id = get_entry_id(entry)
        if entry_id is None:
            logging.warning(f'Post {i} has no ID, skipping details.')
            continue
        logging.debug(f'Retrieving details for post ID {entry_id}...')
        if entry_id in details:
            logging.warning(f'Detail data for post ID {entry_id} already exists. This should not happen!')
            continue

        try:
            r = sess.get(f'{BASE_URL_REST}/{entry_id}', timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            details[entry_id] = r.json()
        except Exception as e:
            logging.error(f'Exception while retrieving details for post ID {entry_id}: {e!r}.')
        # Also wait after a failure, to avoid hammering the server
        if i < post_count-1:
            time.sleep(randint(3,10))
    
    logging.debug(f'Retrieved {len(details)} detail record(s).')
    return details


def generate_feed(data: list, details: dict, output_file: Path|str) -> None:

    fg = FeedGenerator()
    run_timestamp = datetime.now(timezone.utc)
    fg.title('VDAB')
    fg.id(FEED_ID)
    fg.link(href='https://www.vdab.be', rel='alternate')
    fg.lastBuildDate(run_timestamp)
    fg.updated(run_timestamp)
    
    for entry in data:
        entry_id = get_entry_id(entry)
        fe = fg.add_entry()
        # Required fields: if one of these fails, only this entry is dropped
        try:
            if entry_id is None:
                raise ValueError('post has no ID')
            company = entry.get('vacatureBedrijfsnaam')
            title = entry['vacaturefunctie']['naam']
            fe.title(f'{title} ({company})' if company else title)
            link = f'{BASE_URL}/{entry_id}'
            fe.id(link)
            fe.link(href=link)
        except Exception as e:
            logging.error(f'Failed to generate entry for post ID {entry_id}, skipping it: {e!r}')
            fg.remove_entry(fe)
            continue

        # Optional fields: a failure only leaves out that part
        for setter, key in ((fe.published, 'eerstePublicatieDatum'), (fe.updated, 'laatsteWijzigingDatum')):
            try:
                setter(entry.get(key))
            except Exception as e:
                logging.warning(f'Post ID {entry_id}: invalid {key} "{entry.get(key)}": {e!r}')

        detail_data = details.get(entry_id)

        # Build the entry content
        try:
            doc, tag, text, line = Doc().ttl()
            summary = collect_items(entry_id, (
                lambda: f'Bedrijf: {entry['vacatureBedrijfsnaam']} ({entry['leverancier']['type'].lower()})',
                lambda: f'Locatie: {entry['tewerkstellingsLocatieRegioOfAdres'].title()}',
                lambda: f'Type: {entry['vacaturefunctie']['arbeidscircuitLijn']}',
            ))
            if summary:
                with tag('p'):
                    with tag('ul'):
                        for item in summary:
                            line('li', item)

            if isinstance(detail_data, dict):
                sections = (
                    ('Functieomschrijving', lambda: detail_data['functie']['omschrijving']['html']),
                    ('Profiel', lambda: detail_data['profiel']['vereisteKwalificaties']['html']),
                    ('Aanbod', lambda: detail_data['profiel']['aanbod']['aanbodEnVoordelen']['html']),
                )
                for heading, get_html in sections:
                    html = collect_items(entry_id, (get_html,))
                    if html and isinstance(html[0], str) and html[0].strip():
                        line('h3', heading)
                        doc.asis(html[0])

            fe.content(doc.getvalue(), type='html')
        except Exception as e:
            logging.warning(f'Failed to build content for post ID {entry_id}: {e!r}')

    # Do not replace a working feed with an empty one
    entry_count = len(fg.entry())
    if entry_count == 0:
        raise RuntimeError('The feed has no entries, the existing feed file is kept.')

    logging.info(f'Writing {entry_count} entries to {output_file}...')
    fg.atom_file(output_file, pretty=True)
    logging.info(f'RSS feed saved.')


def main(config_file: Path|str, output_file: Path|str, page_count: int, fetch_details: bool=False) -> None:
    logging.info('Retrieving data...')
    data = get_data(config_file, page_count)
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
    parser.add_argument('-p', '--page_count', default=PAGE_COUNT, type=int)
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
             page_count=args.page_count,
             fetch_details=args.details)
    except Exception:
        # A non-zero exit code stops the workflow before it commits the feed
        logging.exception('Feed generation failed.')
        sys.exit(1)
