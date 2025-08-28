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


BASE_URL = 'https://www.vdab.be/vindeenjob/vacatures'
BASE_URL_REST = 'https://www.vdab.be/rest/vindeenjob/v4/vacatures'
PAGE_COUNT = 20


def create_session() -> requests.Session:
    sess = requests.Session()

    sess.headers = {'Content-Type': 'application/json',
               'Accept': 'application/json, text/plain, */*',
               'Referer': 'https://www.vdab.be/vindeenjob/vacatures?locatie=2550%20Kontich&afstand=30&locatieCode=375&sort=standaard&diplomaNiveau=D&arbeidscircuit=8&jobdomein=JOBCAT10&arbeidsduur=V&ervaring=4&arbeidsregime=D&taal=N_15',
               'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15',
               'Vej-Key-Monitor': 'b277002f-e1fa-4fc5-868a-fdab633c3851'
              }
    return sess


def get_date(config_file: Path|str) -> list:
    
    logging.debug('Reading configuration file...')
    with open(config_file) as f:
        config_data = json.load(f)

    data = []
    sess = create_session()
    
    try:
        for i in range(PAGE_COUNT):
            logging.debug(f'Requesting data ({i}/{PAGE_COUNT})...')
            config_data['pagina'] = i
            r = sess.post('https://www.vdab.be/rest/vindeenjob/v4/vacatureLight/zoek', 
                          data=json.dumps(config_data))
            r.raise_for_status()
            partial_data = r.json()['resultaten']
            if len(partial_data) == 0:
                logging.info(f'Bailing out early, no data received for page {i}')
                break
            data.extend(partial_data)
            if i < PAGE_COUNT-1:
                time.sleep(randint(3,10))
    except Exception as e:
        logging.error(f'Exception at attempt {i}: {e!r}. Total entries retrieved: {len(data)}.')
    else:
        logging.debug(f'Data retrieved successfully, {len(data)} records.')
    return data


def get_posting_details(data: list) -> dict:
    
    details = {}
    post_count = len(data)
    sess = create_session()
    
    for i, entry in enumerate(data):
        entry_id = entry['id']['id']
        logging.debug(f'Retrieving details for post ID {entry_id}...')
        if entry_id in details:
            logging.warning(f'Detail data for post ID {entry_id} already exists. This should not happen!')
            continue

        try:
            r = sess.get(f'{BASE_URL_REST}/{entry_id}')
            r.raise_for_status()
            details[entry_id] = r.json()
            if i < post_count-1:
                time.sleep(randint(3,10))
        except Exception as e:
            logging.error(f'Exception while retrieving details for post ID {entry_id}: {e!r}.')
    
    logging.debug(f'Retrieved {len(details)} detail record(s).')
    return details


def generate_feed(data: list, details: dict, output_file: Path|str) -> None:

    fg = FeedGenerator()
    run_timestamp = datetime.now(timezone.utc)
    fg.title('VDAB – Zoek een job')
    fg.link(href='https://www.vdab.be', rel='alternate')
    fg.description('Vind een job in de grootste jobdatabank van Vlaanderen. Snel en gemakkelijk.')
    fg.lastBuildDate(run_timestamp)
    fg.updated(run_timestamp)
    
    for entry in data:
        fe = fg.add_entry()
        entry_id = entry['id']['id']
        try:
            fe.title(f'{entry['vacaturefunctie']['naam']} ({entry['vacatureBedrijfsnaam']})')
            link = f'{BASE_URL}/{entry_id}'
            fe.guid(link, permalink=True)
            fe.link(href=link)
            fe.published(entry['eerstePublicatieDatum'])
            fe.updated(entry['laatsteWijzigingDatum'])

            detail_data = details.get(entry_id)

            # Build the entry content
            doc, tag, text, line = Doc().ttl()
            with tag('p'):
                with tag('ul'):
                    line('li', entry['vacaturefunctie']['naam'])
                    line('li', f'Bedrijf: {entry['vacatureBedrijfsnaam']} ({entry['leverancier']['type'].lower()})')
                    line('li', f'Locatie: {entry['tewerkstellingsLocatieRegioOfAdres'].title()}')
                    line('li', f'Type: {entry['vacaturefunctie']['arbeidscircuitLijn']}')
            
            if detail_data is not None:
                try:
                    line('h3', 'Functieomschrijving')
                    doc.asis(detail_data['functie']['omschrijving']['html'])
                except Exception:
                    pass
                try:
                    line('h3', 'Profiel')
                    doc.asis(detail_data['profiel']['vereisteKwalificaties']['html'])
                except Exception:
                    pass
                try:
                    line('h3', 'Aanbod')
                    doc.asis(detail_data['profiel']['aanbod']['aanbodEnVoordelen']['html'])
                except Exception:
                    pass
            
            fe.description(doc.getvalue())
        except Exception as e:
            logging.error(f'Failed to generate entry for post ID {entry_id}: {e!r}')

    logging.info(f'Writing to {output_file}...')
    fg.rss_file(output_file)
    logging.info(f'RSS feed saved.')


def main(config_file: Path|str, output_file: Path|str) -> None:
    logging.info('Retrieving data...')
    data = get_date(config_file)
    logging.info('Retrieving posting details...')
    details = get_posting_details(data)
    logging.info('Building RSS feed...')
    generate_feed(data, details, output_file)
    logging.info('Done!')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate RSS feeds for VDAB jobs.')
    parser.add_argument('output_file', help='Filename to write the RSS feed to')
    parser.add_argument('config_file', help='Filename containing the request configuration')
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

    main(args.config_file, args.output_file)
