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

__version__ = '0.5.20261012'

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


def load_previous_entries(feed_file: Path|str) -> dict:
    '''Read the entries of the previous feed file and return them by URL.

    Return an empty dict when the file does not exist or cannot be read.
    The script then downloads all postings.
    '''
    path = Path(feed_file)
    if not path.is_file():
        return {}
    try:
        # strip_whitespace=False keeps the text exactly as written, for example a
        # title that ends with a space
        feed = xmltodict.parse(path.read_bytes(), force_list=('entry', 'category'),
                               strip_whitespace=False)['feed']
    except Exception as e:
        logging.warning(f'Failed to read the previous feed {feed_file}, downloading all postings: {e!r}')
        return {}

    entries = {}
    for item in feed.get('entry') or []:
        try:
            content = item.get('content')
            entries[item['id']] = {
                'title': item['title'],
                'published': item.get('published'),
                'updated': datetime.fromisoformat(item['updated']),
                'content': content.get('#text') if isinstance(content, dict) else content,
                'categories': [c['@term'] for c in item.get('category') or [] if c.get('@term')],
            }
        except Exception as e:
            logging.debug(f'Skipping an entry of the previous feed: {e!r}')

    logging.debug(f'Previous feed contains {len(entries)} entries.')
    return entries


def add_posting_details(data: list, previous: dict|None = None) -> None:

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


    # Reuse a posting from the previous feed when its sitemap lastmod did not
    # change. The feed stores lastmod as the <updated> time of the entry.
    previous = previous or {}
    to_fetch = []
    for entry in data:
        cached = previous.get(entry['url'])
        if (cached is not None and entry['lastModification'] is not None
                and cached['updated'] == entry['lastModification']):
            entry['cached'] = cached
        else:
            to_fetch.append(entry)
    logging.info(f'Reusing {len(data) - len(to_fetch)} unchanged posting(s) from the previous feed, '
                 f'downloading {len(to_fetch)}.')

    post_count = len(to_fetch)
    sess = create_session()

    for i, entry in enumerate(to_fetch):
        logging.debug(f'Retrieving details for {entry['url']} ({i+1}/{post_count})...')

        try:
            entry['details'] = extract_data_from_page(sess, entry['url'])
        except Exception as e:
            logging.error(f'Exception while retrieving details for {entry['url']}: {e!r}.')
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


    def add_cached_entry(url: str, cached: dict) -> None:
        '''Add an unchanged posting from the previous feed as it was.'''
        fe = common.add_entry(fg, url, lambda: cached['title'], url)
        if fe is None:
            return
        common.set_optional_date(fe.published, cached.get('published'), url, 'published')
        common.set_optional_date(fe.updated, cached.get('updated'), url, 'updated')
        try:
            if cached.get('content'):
                fe.content(cached['content'], type='html')
            if cached.get('categories'):
                fe.category([{'term': term} for term in cached['categories']])
        except Exception as e:
            logging.warning(f'Failed to reuse content for post {url}: {e!r}')


    fg = common.new_feed('Cartière', FEED_ID, 'https://www.cartiere.be')

    for entry in data:
        if 'cached' in entry:
            add_cached_entry(entry.get('url'), entry['cached'])
            continue

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


def main(output_file: Path|str, refresh: bool=False) -> None:
    logging.info('Retrieving data...')
    data = get_data()
    # The output file still holds the previous feed at this point
    previous = {} if refresh else load_previous_entries(output_file)
    logging.info('Retrieving posting details...')
    add_posting_details(data, previous)
    logging.info('Building RSS feed...')
    generate_feed(data, output_file)
    logging.info('Done!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate RSS feeds for Cartière jobs.')
    parser.add_argument('output_file', help='Filename to write the RSS feed to')
    parser.add_argument('-r', '--refresh', action='store_true',
                        help='Download all postings, also the unchanged ones in the previous feed')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    common.setup_logging(args.verbose)
    common.run(main, output_file=args.output_file, refresh=args.refresh)
