import datetime
import requests
import time


from hn_api import tables
from hn_api import utils


def main(submission_table, params, ts, search_time_query):
    num_processed = 0
    while True:
        try:
            params['numericFilters'] = search_time_query.format(ts)
            response = requests.get(base_url, params)

            data = response.json()
            if data["nbHits"] == 0:
                break
            submissions = data['hits']
            num_submissions = len(submissions)
            ts = submissions[-1]['created_at_i']

            submission_table.insert_records(submissions)

            num_processed += num_submissions
            if num_processed % 10000 == 0:
                print('submissions processed: {}'.format(num_processed))

            time.sleep(rate_limit_wait)
        except Exception as e:
            print(e)
            break
    submission_table.add_indexes()

if __name__ == '__main__':
    with utils.get_connection() as conn:
        submission_table = tables.Submission(conn)
        submission_table.make_table()
        base_url = 'https://hn.algolia.com/api/v1/search_by_date?'
        # Max number of results per Algolia API docs
        hits_per_page = 1000
        search_time_query = 'created_at_i<{}'
        params = {
            'tags': 'story',
            'hitsPerPage': hits_per_page,
            'numericFilters': None
        }

        requests_per_hour = 10000
        rate_limit_wait = 3600 / requests_per_hour
        ts = datetime.datetime.now().strftime('%s')
        main(submission_table, params, ts, search_time_query)
