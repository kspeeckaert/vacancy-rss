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
PAGE_COUNT = 20


def get_date(config_file: Path|str):
    
    logging.debug('Reading configuration file...')
    with open(config_file) as f:
        config_data = json.load(f)
    
    with requests.Session() as s:

        headers = {'Content-Type': 'application/json',
                   'Accept': 'application/json, text/plain, */*',
                   'Referer': 'https://www.vdab.be/vindeenjob/vacatures?locatie=2550%20Kontich&afstand=30&locatieCode=375&sort=standaard&diplomaNiveau=D&arbeidscircuit=8&jobdomein=JOBCAT10&arbeidsduur=V&ervaring=4&arbeidsregime=D&taal=N_15',
                   'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15',
                   'Vej-Key-Monitor': 'b277002f-e1fa-4fc5-868a-fdab633c3851'
                  }
        s.headers = headers
        
        data = []
        
        try:
            for i in range(PAGE_COUNT):
                logging.debug(f'Requesting data ({i}/{PAGE_COUNT})...')
                config_data['pagina'] = i
                r = s.post('https://www.vdab.be/rest/vindeenjob/v4/vacatureLight/zoek', 
                           data=json.dumps(config_data))
                r.raise_for_status()
                data.extend(r.json()['resultaten'])
                if i < PAGE_COUNT-1:
                    time.sleep(randint(3,10))
        except Exception as e:
            logging.error(f'Exception at attempt {i}: {e!r}. Total entries retrieved: {len(data)}.')
        else:
            logging.debug(f'Data retrieved successfully, {len(data)} records.')
        return data


def generate_feed(data: list, output_file: Path|str) -> None:

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

            # Build the entry content
            doc, tag, text, line = Doc().ttl()
            with tag('ul'):
                line('li', entry['vacaturefunctie']['naam'])
                line('li', f'Bedrijf: {entry['vacatureBedrijfsnaam']} ({entry['leverancier']['type'].lower()})')
                line('li', f'Locatie: {entry['tewerkstellingsLocatieRegioOfAdres'].title()}')
                line('li', f'Type: {entry['vacaturefunctie']['arbeidscircuitLijn']}')
            fe.description(doc.getvalue())
            # Add a logo if available
            try:
                fe.enclosure(entry['opmaak']['logo'], type='image/jpeg')
            except Exception:
                pass
        except Exception as e:
            logging.error(f'Failed to generate entry for id {entry_id}: {e!r}')

    logging.info(f'Writing to {output_file}...')
    fg.rss_file(output_file)
    logging.info(f'RSS feed saved.')


def main(config_file: Path|str, output_file: Path|str) -> None:
    data = get_date(config_file)
    generate_feed(data, output_file)


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
