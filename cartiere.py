import argparse
import logging
from pathlib import Path
from datetime import datetime
import requests

from bs4 import BeautifulSoup
import xmltodict
import json
from yattag import Doc

import common

__version__ = '0.4.20261012'

URL_JOB_LISTING = 'https://www.cartiere.be/matador-job-listings-sitemap.xml'
FEED_ID = '9069cb01-9b04-4d24-9a64-8e1fedbf261f'


def create_session() -> requests.Session:
    return common.create_session({'Content-Type': 'application/json',
                                  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                                  'Referer': 'https://www.cartiere.be/matador-job-listings-sitemap.xml',
                                  'User-Agent': common.USER_AGENT,
                                  'Host': 'www.cartiere.be'
                                 })


def get_data() -> list:

    sess = create_session()

    try:
        logging.debug('Fetching sitemap data...')
        r = sess.get(URL_JOB_LISTING, timeout=common.REQUEST_TIMEOUT)
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
            r = sess.get(url, timeout=common.REQUEST_TIMEOUT)
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
        # Also wait after a failure
        common.pause_between_requests(i, post_count)


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


    fg = common.new_feed('Cartière', FEED_ID, 'https://www.cartiere.be')

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

        # Required fields: if one of these fails, only this entry is dropped.
        # No unique ID available, so we'll use the URL.
        url = entry.get('url')
        fe = common.add_entry(fg, url, lambda: details['title'], url)
        if fe is None:
            continue

        # Optional fields: a failure only leaves out that part
        common.set_optional_date(fe.published, details.get('datePosted'), url, 'datePosted')
        common.set_optional_date(fe.updated, entry.get('lastModification'), url, 'lastmod')

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

            doc = Doc()
            common.add_list(doc, summary)

            description = details.get('description')
            if isinstance(description, str):
                doc.asis(description)
            fe.content(doc.getvalue(), type='html')
        except Exception as e:
            logging.warning(f'Failed to build content for post {url}: {e!r}')

        # Split occupational categories string (if available) into separate entries
        try:
            categories = [{'term': category.strip()} for category in details['occupationalCategory'].split(',') if category.strip()]
            if categories:
                fe.category(categories)
        except Exception as e:
            logging.debug(f'Post {url}: categories left out: {e!r}')

    common.write_feed(fg, output_file)


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

    common.setup_logging(args.verbose)
    common.run(main, output_file=args.output_file)
