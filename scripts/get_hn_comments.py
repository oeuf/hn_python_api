import datetime
import requests
import time

from hn_api import tables
from hn_api import utils


def main(comment_table, params, ts, search_time_query):
    num_processed = 0
    while True:
        try:
            params['numericFilters'] = search_time_query.format(ts)
            response = requests.get(base_url, params)
            data = response.json()
            if data["nbHits"] == 0:
                break
            comments = data['hits']
            num_comments = len(comments)
            ts = comments[-1]['created_at_i']

            comment_table.insert_records(comments)

            num_processed += num_comments
            if num_processed % 10000 == 0:
                print('Comments processed: {}'.format(num_processed))

            time.sleep(rate_limit_wait)

        except Exception as e:
            print(e)
            break


if __name__ == '__main__':
    with utils.get_connection() as conn:
        comment_table = tables.Comment(conn)
        comment_table.make_table()
        base_url = 'https://hn.algolia.com/api/v1/search_by_date?'
        # Max number of results per Algolia API docs
        hits_per_page = 1000
        search_time_query = 'created_at_i<{}'
        params = {
            'tags': 'comment',
            'hitsPerPage': hits_per_page,
            'numericFilters': None
        }

        requests_per_hour = 10000
        rate_limit_wait = 3600 / requests_per_hour
        ts = datetime.datetime.now().strftime('%s')
        main(comment_table, params, ts, search_time_query)
