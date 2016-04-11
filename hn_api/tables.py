import datetime


class Column(object):
    def __init__(self, column_name, column_type):
        self.name = column_name
        self.column_type = column_type

    def __str__(self):
        return '{} {}'.format(self.name, self.column_type)

    def __iter__(self):
        return iter((self.name, self.column_type))

    def _get_nullable_id(self, doc, key):
        if doc[key] is None:
            return None
        else:
            return int(doc[key])


class Table(object):
    TABLENAME = None
    COLUMNS = None

    def __init__(self, conn):
        self.conn = conn

    def make_table(self):
        with self.conn.cursor() as cur:
            query = """CREATE TABLE IF NOT EXISTS {} ({})""".format(
                self.TABLENAME, ','.join(map(str, self.COLUMNS)))
            cur.execute(query)
        self.conn.commit()

    def insert_records(self, records):
        query, args = self._get_query_args(records)
        with self.conn.cursor() as cur:
            cur.execute(query, args)
        self.conn.commit()

    def vacuum(self):
        query = 'VACUUM ANALYZE {}'.format(self.TABLENAME)
        with self.conn.cursor() as cur:
            cur.execute(query)

    @classmethod
    def header(cls):
        return [column.name for column in cls.COLUMNS]


class Comment(Table):
    TABLENAME = 'comment'
    COLUMNS = [
        Column('id', 'int PRIMARY KEY'),
        Column('created_at', 'timestamp'),
        Column('story_id', 'int'),
        Column('parent_id', 'int'),
        Column('comment_text', 'text'),
        Column('num_points', 'int'),
        Column('author', 'text'),
    ]

    def _get_query_args(self, comments):
        args = []
        records_template = ','.join(['%s'] * len(comments))
        query = """INSERT INTO {0} (
            id,
            created_at,
            story_id,
            parent_id,
            comment_text,
            num_points,
            author) VALUES {1}
            ON CONFLICT (id) DO UPDATE SET
                comment_text = excluded.comment_text,
                num_points = excluded.num_points;""".format(self.TABLENAME, records_template)

        for comment in comments:
            if 'parent_id' in comment:
                comment_text = comment['comment_text']
                created_at = datetime.datetime.fromtimestamp(int(comment['created_at_i']))
                object_id = self._get_nullable_id(comment, 'object_id')
                parent_id = self._get_nullable_id(comment, 'parent_id')
                story_id = self._get_nullable_id(comment, 'story_id')

                args.append((
                    object_id,
                    created_at,
                    story_id,
                    parent_id,
                    comment_text,
                    comment['points'],
                    comment['author'],
                ))

        return query, args

    def add_indexes(self):
        with self.conn.cursor() as cur:
            cur.execute('CREATE UNIQUE INDEX id_commentx ON comment (id);')
            cur.execute('CREATE INDEX created_at_commentx ON comment (created_at);')
            cur.execute('CREATE INDEX story_id_commentx ON comment (story_id);')
        self.conn.commit()


class Submission(Table):
    TABLENAME = 'submission'
    COLUMNS = [
        Column('id', 'int PRIMARY KEY'),
        Column('created_at', 'timestamp'),
        Column('title', 'text'),
        Column('url', 'text'),
        Column('author', 'text'),
        Column('num_points', 'int'),
        Column('num_comments', 'int'),
    ]

    def _get_query_args(self, submissions):
        args = []
        records_template = ','.join(['%s'] * len(submissions))
        query = """INSERT INTO {0} (
            id,
            created_at,
            title,
            url,
            author,
            num_points,
            num_comments) VALUES {1}
            ON CONFLICT (id) DO UPDATE SET
                title = excluded.title,
                url = excluded.url,
                num_points = excluded.num_points,
                num_comments = excluded.num_comments;""".format(self.TABLENAME, records_template)

        for submission in submissions:
            created_at = datetime.datetime.fromtimestamp(int(submission['created_at_i']))

            args.append((
                int(submission['objectID']),
                created_at,
                submission['title'],
                submission['url'],
                submission['author'],
                submission['points'],
                submission['num_comments'],
            ))

        return query, args

    def add_indexes(self):
        with self.conn.cursor() as cur:
            cur.execute('CREATE UNIQUE INDEX id_submissionx ON submission (id);')
            cur.execute('CREATE INDEX created_at_submissionx ON submission (created_at);')
        self.conn.commit()
