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

from bs4 import BeautifulSoup
import xmltodict
import json
from yattag import Doc

__version__ = '0.2.20261012'

URL_JOB_LISTING = 'https://www.cartiere.be/matador-job-listings-sitemap.xml'
FEED_ID = '9069cb01-9b04-4d24-9a64-8e1fedbf261f'
# (connect, read) timeout in seconds for every HTTP request
REQUEST_TIMEOUT = (10, 60)


def create_session() -> requests.Session:
    sess = requests.Session()

    sess.headers = {'Content-Type': 'application/json',
                              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                              'Referer': 'https://www.cartiere.be/matador-job-listings-sitemap.xml',
                              'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15',
                              'Host': 'www.cartiere.be'}
    # Retry transient failures (connection errors, rate limiting, server errors)
    retry = Retry(total=3, backoff_factor=2,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=None)
    sess.mount('https://', HTTPAdapter(max_retries=retry))
    return sess


def get_data() -> list:

    sess = create_session()
    
    try:
        logging.debug('Fetching sitemap data...')
        r = sess.get(URL_JOB_LISTING, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        # force_list keeps 'url' a list, also when the sitemap has only one entry
        job_listing = xmltodict.parse(r.content, force_list=('url',))['urlset']['url']
    except Exception as e:
        logging.error(f'Failed to retrieve data: {e!r}')
        raise

    logging.debug(f'Sitemap contains {len(job_listing)} entries...')
    data = []
    # Extract urls from the sitemap XML
    for url in job_listing:
        try:
            if url['loc'] == 'https://www.cartiere.be/jobs/':
                # Skip dummy entry
                continue
            try:
                last_modification = datetime.fromisoformat(url['lastmod'])
            except (KeyError, TypeError, ValueError):
                last_modification = None
            data.append({'url': url['loc'], 'lastModification': last_modification})
        except Exception as e:
            logging.warning(f'Skipping invalid sitemap entry {url!r}: {e!r}')

    logging.debug(f'Data retrieved successfully, {len(data)} records.')
    return data


def add_posting_details(data: list) -> None:
    
    def extract_data_from_page(sess: requests.Session, url:str ) -> dict:

        try:
            r = sess.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            # Take the page contents and extract the LD JSON data
            soup = BeautifulSoup(r.text, 'lxml')
            ld_json = soup.find('script', class_='yoast-schema-graph', type='application/ld+json').string
            # Deserialize JSON
            raw_data = json.loads(str(ld_json))
            # Only retain the actual job posting
            data = [item for item in raw_data['@graph'] if item.get('@type') == 'JobPosting'][0]
            # Both dates are optional in schema.org, so a missing or invalid date becomes None
            for key in ('datePosted', 'validThrough'):
                try:
                    data[key] = datetime.fromisoformat(data[key])
                except (KeyError, TypeError, ValueError):
                    data[key] = None
        except Exception as e:
            logging.warning(f'Failed to retrieve data from webpage {url}: {e!r}')
            raise
        return data


    post_count = len(data)
    sess = create_session()
    
    for i, entry in enumerate(data):
        logging.debug(f'Retrieving details for post {i}...')
        
        try:
            entry['details'] = extract_data_from_page(sess, entry['url'])
        except Exception as e:
            logging.error(f'Exception while retrieving details for post {i}: {e!r}.')
        # Also wait after a failure, to avoid hammering the server
        if i < post_count-1:
            time.sleep(randint(3,10))


def generate_feed(data: list, output_file: Path|str) -> None:

    def get_address(listing: dict) -> str:
        
        fields = []
        try:
            address_entry = listing['details']['jobLocation']['address']
            for key in ('streetAddress', 'postalCode', 'addressLocality', 'addressRegion', 'addressCountry'):
                value = address_entry.get(key)
                # schema.org also allows objects here (for example a Country), keep only text
                if isinstance(value, str) and value.strip():
                    fields.append(value.strip())

        except Exception as e:
            logging.warning(f'Failed to extract address: {e!r}')
            return None

        return ', '.join(fields)


    fg = FeedGenerator()
    run_timestamp = datetime.now(timezone.utc)
    fg.title('Cartière')
    fg.id(FEED_ID)
    fg.link(href='https://www.cartiere.be', rel='alternate')
    fg.lastBuildDate(run_timestamp)
    fg.updated(run_timestamp)
    
    for entry in data:
        details = entry.get('details')
        if not isinstance(details, dict):
            # Without the job details, there's no point in adding this
            continue
        # schema.org allows one employment type or a list of them
        employment_type = details.get('employmentType')
        employment_types = employment_type if isinstance(employment_type, list) else [employment_type]
        if 'FREELANCE' in employment_types:
            # Not interested in freelance offers
            continue

        fe = fg.add_entry()
        # Required fields: if one of these fails, only this entry is dropped
        try:
            fe.title(details['title'])
            # No unique ID available, so we'll use the URL
            fe.id(entry['url'])
            fe.link(href=entry['url'])
        except Exception as e:
            logging.error(f'Failed to generate entry for post {entry.get('url')}, skipping it: {e!r}')
            fg.remove_entry(fe)
            continue

        # Optional fields: a failure only leaves out that part
        for setter, value in ((fe.published, details.get('datePosted')), (fe.updated, entry.get('lastModification'))):
            try:
                setter(value)
            except Exception as e:
                logging.warning(f'Post {entry['url']}: invalid date "{value}": {e!r}')

        # Build the entry content
        try:
            summary = []
            if address := get_address(entry):
                summary.append(f'Locatie: {address}')
            if details.get('datePosted'):
                summary.append(f'Published on {details['datePosted']}')
            if entry.get('lastModification'):
                summary.append(f'Last change {entry['lastModification']}')
            if details.get('validThrough'):
                summary.append(f'Valid until {details['validThrough']}')

            doc, tag, text, line = Doc().ttl()
            if summary:
                with tag('p'):
                    with tag('ul'):
                        for item in summary:
                            line('li', item)

            description = details.get('description')
            if isinstance(description, str):
                doc.asis(description)
            fe.content(doc.getvalue(), type='html')
        except Exception as e:
            logging.warning(f'Failed to build content for post {entry['url']}: {e!r}')

        # Split occupational categories string (if available) into separate entries
        try:
            categories = [{'term': category.strip()} for category in details['occupationalCategory'].split(',') if category.strip()]
            if categories:
                fe.category(categories)
        except Exception as e:
            logging.debug(f'Post {entry['url']}: categories left out: {e!r}')

    # Do not replace a working feed with an empty one
    entry_count = len(fg.entry())
    if entry_count == 0:
        raise RuntimeError('The feed has no entries, the existing feed file is kept.')

    logging.info(f'Writing {entry_count} entries to {output_file}...')
    fg.atom_file(output_file, pretty=True)
    logging.info(f'RSS feed saved.')


def main(output_file: Path|str) -> None:
    logging.info('Retrieving data...')
    data = get_data()
    logging.info('Retrieving posting details...')
    add_posting_details(data)
    logging.info('Building RSS feed...')
    generate_feed(data, output_file)
    logging.info('Done!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate RSS feeds for Cartière jobs.')
    parser.add_argument('output_file', help='Filename to write the RSS feed to')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()

    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    # Avoid logging from requests
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    try:
        main(output_file=args.output_file)
    except Exception:
        # A non-zero exit code stops the workflow before it commits the feed
        logging.exception('Feed generation failed.')
        sys.exit(1)
