'''Shared helpers for the vacancy feed scripts.'''
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from random import randint

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from feedgen.feed import FeedGenerator

__version__ = '0.4.20261012'

# (connect, read) timeout in seconds for every HTTP request
REQUEST_TIMEOUT = (10, 60)
USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Safari/605.1.15'


# ---------- HTTP ----------

def create_session(headers: dict) -> requests.Session:
    '''Return a session that sends these headers and retries transient failures.'''
    sess = requests.Session()
    sess.headers = headers
    # Retry transient failures (connection errors, rate limiting, server errors).
    # allowed_methods=None also retries POST. The scripts only use POST for
    # searches, which do not change data.
    retry = Retry(total=3, backoff_factor=2,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=None)
    sess.mount('https://', HTTPAdapter(max_retries=retry))
    return sess


def pause_between_requests(index: int, count: int) -> None:
    '''Wait 3 to 10 seconds after each request except the last, to avoid hammering the server.'''
    if index < count - 1:
        time.sleep(randint(3, 10))


def fetch_details(sess: requests.Session, data: list, get_id, detail_url) -> dict:
    '''Download the JSON details of each post and return them by post ID.

    get_id(entry) returns the post ID or None. detail_url(post_id) returns
    the URL to download. A failed request only leaves out that post.
    '''
    details = {}
    post_count = len(data)

    for i, entry in enumerate(data):
        entry_id = get_id(entry)
        if entry_id is None:
            logging.warning(f'Post {i} has no ID, skipping details.')
            continue
        logging.debug(f'Retrieving details for post ID {entry_id}...')
        if entry_id in details:
            logging.warning(f'Detail data for post ID {entry_id} already exists. This should not happen!')
            continue

        try:
            r = sess.get(detail_url(entry_id), timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            details[entry_id] = r.json()
        except Exception as e:
            logging.error(f'Exception while retrieving details for post ID {entry_id}: {e!r}.')
        # Also wait after a failure
        pause_between_requests(i, post_count)

    logging.debug(f'Retrieved {len(details)} detail record(s).')
    return details


# ---------- Feed ----------

def new_feed(title: str, feed_id: str, link: str) -> FeedGenerator:
    '''Return a feed with its header fields and the run time as update time.'''
    fg = FeedGenerator()
    run_timestamp = datetime.now(timezone.utc)
    fg.title(title)
    fg.id(feed_id)
    fg.link(href=link, rel='alternate')
    fg.lastBuildDate(run_timestamp)
    fg.updated(run_timestamp)
    return fg


def add_entry(fg: FeedGenerator, post_id, get_title, link: str):
    '''Add an entry with the required fields: title, ID and link.

    get_title() returns the title. If a required field fails, the entry is
    removed again and the function returns None, so only this entry is lost.
    '''
    fe = fg.add_entry()
    try:
        if post_id is None:
            raise ValueError('post has no ID')
        fe.title(get_title())
        fe.id(link)
        fe.link(href=link)
    except Exception as e:
        logging.error(f'Failed to generate entry for post {post_id}, skipping it: {e!r}')
        fg.remove_entry(fe)
        return None
    return fe


def set_optional_date(setter, value, post_id, label: str) -> None:
    '''Set an optional date. An invalid date is logged and left out.'''
    try:
        setter(value)
    except Exception as e:
        logging.warning(f'Post {post_id}: invalid {label} "{value}": {e!r}')


def collect_items(post_id, items) -> list:
    '''Evaluate each item function and keep the results that do not fail.'''
    result = []
    for item in items:
        try:
            result.append(item())
        except Exception as e:
            logging.debug(f'Post {post_id}: field left out: {e!r}')
    return result


def add_list(doc, items: list) -> None:
    '''Add the items to the yattag document as a bulleted list. Add nothing when there are no items.'''
    if not items:
        return
    with doc.tag('p'):
        with doc.tag('ul'):
            for item in items:
                doc.line('li', item)


def write_feed(fg: FeedGenerator, output_file: Path|str) -> None:
    '''Write the feed as Atom. Raise an error instead of writing an empty feed.'''
    # Do not replace a working feed with an empty one
    entry_count = len(fg.entry())
    if entry_count == 0:
        raise RuntimeError('The feed has no entries, the existing feed file is kept.')

    logging.info(f'Writing {entry_count} entries to {output_file}...')
    fg.atom_file(output_file, pretty=True)
    logging.info('RSS feed saved.')


# ---------- Command line ----------

def setup_logging(verbose: bool) -> None:
    '''Log to the console, and leave out the debug messages of requests and urllib3.'''
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def require_file(path: Path|str) -> None:
    '''Exit with code 1 when the file does not exist.'''
    if not Path(path).is_file():
        logging.error(f'File not found: {path}')
        sys.exit(1)


def run(main, **kwargs) -> None:
    '''Run main(**kwargs) and exit with code 1 when it fails.

    A non-zero exit code stops the workflow before it commits the feed.
    '''
    try:
        main(**kwargs)
    except Exception:
        logging.exception('Feed generation failed.')
        sys.exit(1)
