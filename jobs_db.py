import sqlite3
from contextlib import closing
import hashlib, json, datetime

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    title TEXT,
    company TEXT,
    location TEXT,
    url TEXT,
    date_posted TEXT,
    date_scraped TEXT,
    source TEXT,
    description TEXT,
    raw_json TEXT
);
"""

class JobDB:
    def __init__(self, path='jobs.db'):
        self.path = path
        # Create table on init, but don't hold the connection
        self._create_table()

    def _get_conn(self):
        """
        Helper function to get a new connection.
        This is the key fix: each thread gets its own connection
        only when it needs it.
        """
        return sqlite3.connect(self.path)

    def _create_table(self):
        """Creates the table if it doesn't exist."""
        with closing(self._get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute(CREATE_TABLE_SQL)
                conn.commit()

    def _make_id(self, job):
        url = job.get('url','') or (job.get('title','') + (job.get('company') or ''))
        return hashlib.sha256(url.encode('utf-8')).hexdigest()

    def add_job(self, job):
        jid = self._make_id(job)
        now = datetime.datetime.utcnow().isoformat()
        
        # Get a fresh connection just for this one transaction
        with closing(self._get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                try:
                    cur.execute("""INSERT INTO jobs (id,title,company,location,url,date_posted,date_scraped,source,description,raw_json)
                                VALUES (?,?,?,?,?,?,?,?,?,?)""", (
                        jid,
                        job.get('title'),
                        job.get('company'),
                        job.get('location'),
                        job.get('url'),
                        job.get('date_posted'),
                        now,
                        job.get('source'),
                        job.get('description'),
                        json.dumps(job.get('raw', {}))
                    ))
                    conn.commit()
                    return True
                except sqlite3.IntegrityError:
                    return False # Job already exists

    def recent_jobs(self, limit=100):
        # Get a fresh connection just for this one query
        with closing(self._get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                cur.execute("SELECT title,company,location,url,date_posted,source FROM jobs ORDER BY date_scraped DESC LIMIT ?", (limit,))
                rows = cur.fetchall()
                cols = ['title','company','location','url','date_posted','source']
                return [dict(zip(cols,r)) for r in rows]

    def close(self):
        # No persistent connection to close anymore
        pass