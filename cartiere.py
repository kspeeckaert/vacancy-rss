import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
from random import randint
import time
import requests
from feedgen.feed import FeedGenerator

from bs4 import BeautifulSoup
import xmltodict
import json
from yattag import Doc


URL_JOB_LISTING = 'https://www.cartiere.be/matador-job-listings-sitemap.xml'
FEED_ID = '9069cb01-9b04-4d24-9a64-8e1fedbf261f'


def create_session() -> requests.Session:
    sess = requests.Session()

    sess.headers = headers = {'Content-Type': 'application/json',
                              'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                              'Referer': 'https://www.cartiere.be/matador-job-listings-sitemap.xml',
                              'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15',
                              'Host': 'www.cartiere.be'}
    return sess


def get_data() -> list:

    sess = create_session()
    
    try:
        logging.debug('Fetching sitemap data...')
        r = sess.get(URL_JOB_LISTING)
        r.raise_for_status()
        job_listing = xmltodict.parse(r.content)['urlset']['url']
        logging.debug(f'Sitemap contains {len(job_listing)} entries...')
        data = []
        # Extract urls from the sitemap XML
        for url in job_listing:
            if url['loc'] == 'https://www.cartiere.be/jobs/':
                # Skip dummy entry
                continue
            data.append({'url': url['loc'], 'lastModification': datetime.fromisoformat(url['lastmod'])})

    except Exception as e:
        logging.error(f'Failed to retrieve data: {e!r}')
        raise

    logging.debug(f'Data retrieved successfully, {len(data)} records.')
    return data


def add_posting_details(data: list) -> None:
    
    def extract_data_from_page(sess: requests.Session, url:str ) -> dict:

        try:
            r = sess.get(url)
            r.raise_for_status()
            # Take the page contents and extract the LD JSON data
            soup = BeautifulSoup(r.text, 'lxml')
            ld_json = soup.find('script', class_='yoast-schema-graph', type='application/ld+json').string
            # Deserialize JSON
            raw_data = json.loads(str(ld_json))
            # Only retain the actual job posting
            data = [item for item in raw_data['@graph'] if item['@type'] == 'JobPosting'][0]
            data['datePosted'] = datetime.fromisoformat(data['datePosted'])
            data['validThrough'] = datetime.fromisoformat(data['validThrough'])
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
            if i < post_count-1:
                time.sleep(randint(3,10))
        except Exception as e:
            logging.error(f'Exception while retrieving details for post {i}: {e!r}.')


def generate_feed(data: list, output_file: Path|str) -> None:

    def get_address(listing: dict) -> str:
        
        fields = []
        try:
            address_entry = listing['details']['jobLocation']['address']
            for key in ('streetAddress', 'postalCode', 'addressLocality', 'addressRegion', 'addressCountry'):
                try:
                    fields.append(address_entry[key])
                except KeyError:
                    pass

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
        if 'details' not in entry:
            # Without the job details, there's no point in adding this
            continue
        if entry['details']['employmentType'] == 'FREELANCE':
            # Not interested in freelance offers
            continue

        fe = fg.add_entry()
        try:
            fe.title(entry['details']['title'])
            # No unique ID available, so we'll use the URL
            fe.id(entry['url'])
            fe.link(href=entry['url'])
            fe.published(entry['details']['datePosted'])
            fe.updated(entry['lastModification'])

            # Build the entry content
            doc, tag, text, line = Doc().ttl()
            with tag('p'):
                with tag('ul'):                    
                    line('li', f'Locatie: {get_address(entry)}')
                    line('li', f'Published on {entry['details']['datePosted']}, last change {entry['lastModification']}')
                    line('li', f'Valid until {entry['details']['validThrough']}')
            
            doc.asis(entry['details']['description'])
            fe.content(doc.getvalue(), type='html')
            # Split occupational categories string (if available) into separate entries
            try:
                categories = [{'term': category} for category in entry['details']['occupationalCategory'].split(',')]
                fe.category(categories)
            except Exception:
                pass
        except Exception as e:
            logging.error(f'Failed to generate entry for post {entry['url']}: {e!r}')

    logging.info(f'Writing to {output_file}...')
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
    parser = argparse.ArgumentParser(description='Generate RSS feeds for VDAB jobs.')
    parser.add_argument('output_file', help='Filename to write the RSS feed to')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()

    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    # Avoid logging from requests
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    main(output_file=args.output_file)
