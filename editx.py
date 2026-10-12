import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone
import math
import requests

import json
from yattag import Doc

import common

__version__ = '0.4.20261012'

BASE_URL = 'https://editx.eu/en/it-jobs'
BASE_URL_REST = 'https://api.editx.eu/en/api/editx/jobs'
BASE_URL_DETAIL_REST = 'https://api.editx.eu/en/api/editx/job'
FEED_ID = 'd34c4e59-e6f2-4fce-8ea6-e0da265fbf22'


def create_session() -> requests.Session:
    return common.create_session({'Content-Type': 'application/json',
                                  'Accept': 'application/json',
                                  'Origin': 'https://editx.eu',
                                  'Referer': 'https://editx.eu/',
                                  'User-Agent': common.USER_AGENT
                                 })


def get_data(config_file: Path|str) -> list:

    logging.debug('Reading configuration file...')
    with open(config_file) as f:
        config_data = json.load(f)

    sess = create_session()

    try:
        r = sess.get(BASE_URL_REST,
                     params=config_data,
                     timeout=common.REQUEST_TIMEOUT)
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
                            timeout=common.REQUEST_TIMEOUT)
                r.raise_for_status()
                data.extend(r.json()['jobs'])
            except Exception as e:
                # Keep the pages we already have
                logging.error(f'Failed to retrieve page {i+1}/{page_count}, continuing with {len(data)} records: {e!r}')
                break

    logging.debug(f'Data retrieved successfully, {len(data)} records.')
    return data


def get_posting_details(data: list) -> dict:
    return common.fetch_details(create_session(), data,
                                get_id=lambda entry: entry.get('id'),
                                detail_url=lambda entry_id: f'{BASE_URL_DETAIL_REST}/{entry_id}')


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

    def get_title(entry):
        company = (entry.get('company') or {}).get('label')
        return f'{entry['title']} ({company})' if company else entry['title']

    fg = common.new_feed('editx', FEED_ID, 'https://www.editx.eu')

    for entry in data:
        entry_id = entry.get('id')
        # Required fields: if one of these fails, only this entry is dropped
        fe = common.add_entry(fg, entry_id, lambda: get_title(entry), f'{BASE_URL}/{entry_id}')
        if fe is None:
            continue

        # Optional fields: a failure only leaves out that part
        common.set_optional_date(fe.published, get_native_ts(entry.get('onlineDate')), entry_id, 'onlineDate')
        common.set_optional_date(fe.updated, get_native_ts(entry.get('changed')), entry_id, 'changed')

        detail_data = details.get(entry_id)

        # Build the entry content
        try:
            doc = Doc()
            common.add_list(doc, common.collect_items(entry_id, (
                lambda: f'Bedrijf: {entry['company']['label']}',
                lambda: f'Recruiter: {entry['recruiter']['firstname']} {entry['recruiter']['lastname']}, {entry['recruiter']['position']['position']['label']} – {entry['recruiter']['position']['organization']['label']}',
                lambda: f'Locatie: {entry['locality']} ({entry.get('address')})',
                lambda: f'Published on {entry['onlineDate']['label']}, last change {entry['changed']['label']}',
            )))

            if isinstance(detail_data, dict):
                for heading, key in (('Rol', 'role'), ('Profiel', 'profile'), ('Aanbod', 'proposal')):
                    html = detail_data.get(key)
                    if isinstance(html, str) and html.strip():
                        doc.line('h3', heading)
                        doc.asis(html)
                company_items = common.collect_items(entry_id, (
                    lambda: f'Adres: {detail_data['company']['address']}',
                    lambda: f'Vacatures: {detail_data['company']['countJobOnline']}',
                    lambda: f'Werknemers: {detail_data['company']['numberOfEmployees']['label']}',
                    lambda: f'Sector: {detail_data['company']['industry']['label']}',
                ))
                if company_items:
                    doc.line('h3', 'Bedrijf')
                    common.add_list(doc, company_items)

            fe.content(doc.getvalue(), type='html')
        except Exception as e:
            logging.warning(f'Failed to build content for post ID {entry_id}: {e!r}')

        try:
            categories = [{'term': skill['label']} for skill in entry['skills']]
            if categories:
                fe.category(categories)
        except Exception as e:
            logging.debug(f'Post {entry_id}: categories left out: {e!r}')

    common.write_feed(fg, output_file)


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

    common.setup_logging(args.verbose)
    common.require_file(args.config_file)
    common.run(main,
               config_file=args.config_file,
               output_file=args.output_file,
               fetch_details=args.details)
