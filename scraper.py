import requests
from jobs_db import JobDB

REMOTIVE_API = 'https://remotive.com/api/remote-jobs'

def fetch_remotive(keyword=None):
    try:
        params = {}
        if keyword:
            params['search'] = keyword
        r = requests.get(REMOTIVE_API, params=params, timeout=15, headers={'User-Agent':'Mozilla/5.0'})
        if r.status_code != 200:
            print('Remotive returned', r.status_code)
            return []
        data = r.json()
        jobs = []
        for item in data.get('jobs', [])[:50]:
            jobs.append({
                'title': item.get('title'),
                'company': item.get('company_name'),
                'location': item.get('candidate_required_location'),
                'url': item.get('url'),
                'date_posted': item.get('publication_date'),
                'description': item.get('description'),
                'source': 'remotive',
                'raw': item
            })
        return jobs
    except Exception as e:
        print('Remotive fetch failed:', e)
        return []

def run_scraper_once(db_path='jobs.db', keywords=None):
    if keywords is None:
        keywords = ['python','backend','data']
    db = JobDB(db_path)
    all_new = 0
    for kw in keywords:
        jobs = fetch_remotive(kw)
        for job in jobs:
            if db.add_job(job):
                all_new += 1
    print(f'Scraper: added {all_new} new jobs')

def schedule_scraper_in_background(interval_minutes=30, db_path='jobs.db'):
    import schedule, time
    def job():
        run_scraper_once(db_path=db_path)
    schedule.clear()
    schedule.every(interval_minutes).minutes.do(job)
    # run once at start
    job()
    while True:
        schedule.run_pending()
        time.sleep(1)
