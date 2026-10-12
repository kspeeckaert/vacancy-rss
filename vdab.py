import argparse
import logging
from pathlib import Path
import requests

import json
from yattag import Doc

import common

__version__ = '0.4.20261012'

BASE_URL = 'https://www.vdab.be/vindeenjob/vacatures'
BASE_URL_REST = 'https://www.vdab.be/rest/vindeenjob/v4/vacatures'
SEARCH_URL_REST = 'https://www.vdab.be/rest/vindeenjob/v4/vacatureLight/zoek'
PAGE_COUNT = 20
FEED_ID = 'e2590d85-368a-4ef9-863c-931baa40fcec'


def create_session() -> requests.Session:
    return common.create_session({'Content-Type': 'application/json',
                                  'Accept': 'application/json, text/plain, */*',
                                  'Referer': 'https://www.vdab.be/vindeenjob/vacatures?locatie=2550%20Kontich&afstand=30&locatieCode=375&sort=standaard&diplomaNiveau=D&arbeidscircuit=8&jobdomein=JOBCAT10&arbeidsduur=V&ervaring=4&arbeidsregime=D&taal=N_15',
                                  'User-Agent': common.USER_AGENT,
                                  'Vej-Key-Monitor': 'b277002f-e1fa-4fc5-868a-fdab633c3851'
                                 })


def get_data(config_file: Path|str, page_count: int) -> list:

    logging.debug('Reading configuration file...')
    with open(config_file) as f:
        config_data = json.load(f)

    data = []
    sess = create_session()

    try:
        for i in range(page_count):
            logging.debug(f'Requesting data ({i+1}/{page_count})...')
            config_data['pagina'] = i
            r = sess.post(SEARCH_URL_REST,
                          data=json.dumps(config_data),
                          timeout=common.REQUEST_TIMEOUT)
            r.raise_for_status()
            partial_data = r.json()['resultaten']
            if len(partial_data) == 0:
                logging.info(f'Bailing out early, no data received for page {i}')
                break
            data.extend(partial_data)
            common.pause_between_requests(i, page_count)
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


def get_posting_details(data: list) -> dict:
    return common.fetch_details(create_session(), data,
                                get_id=get_entry_id,
                                detail_url=lambda entry_id: f'{BASE_URL_REST}/{entry_id}')


def generate_feed(data: list, details: dict, output_file: Path|str) -> None:

    def get_title(entry):
        company = entry.get('vacatureBedrijfsnaam')
        title = entry['vacaturefunctie']['naam']
        return f'{title} ({company})' if company else title

    fg = common.new_feed('VDAB', FEED_ID, 'https://www.vdab.be')

    for entry in data:
        entry_id = get_entry_id(entry)
        # Required fields: if one of these fails, only this entry is dropped
        fe = common.add_entry(fg, entry_id, lambda: get_title(entry), f'{BASE_URL}/{entry_id}')
        if fe is None:
            continue

        # Optional fields: a failure only leaves out that part
        common.set_optional_date(fe.published, entry.get('eerstePublicatieDatum'), entry_id, 'eerstePublicatieDatum')
        common.set_optional_date(fe.updated, entry.get('laatsteWijzigingDatum'), entry_id, 'laatsteWijzigingDatum')

        detail_data = details.get(entry_id)

        # Build the entry content
        try:
            doc = Doc()
            common.add_list(doc, common.collect_items(entry_id, (
                lambda: f'Bedrijf: {entry['vacatureBedrijfsnaam']} ({entry['leverancier']['type'].lower()})',
                lambda: f'Locatie: {entry['tewerkstellingsLocatieRegioOfAdres'].title()}',
                lambda: f'Type: {entry['vacaturefunctie']['arbeidscircuitLijn']}',
            )))

            if isinstance(detail_data, dict):
                sections = (
                    ('Functieomschrijving', lambda: detail_data['functie']['omschrijving']['html']),
                    ('Profiel', lambda: detail_data['profiel']['vereisteKwalificaties']['html']),
                    ('Aanbod', lambda: detail_data['profiel']['aanbod']['aanbodEnVoordelen']['html']),
                )
                for heading, get_html in sections:
                    html = common.collect_items(entry_id, (get_html,))
                    if html and isinstance(html[0], str) and html[0].strip():
                        doc.line('h3', heading)
                        doc.asis(html[0])

            fe.content(doc.getvalue(), type='html')
        except Exception as e:
            logging.warning(f'Failed to build content for post ID {entry_id}: {e!r}')

    common.write_feed(fg, output_file)


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

    common.setup_logging(args.verbose)
    common.require_file(args.config_file)
    common.run(main,
               config_file=args.config_file,
               output_file=args.output_file,
               page_count=args.page_count,
               fetch_details=args.details)
