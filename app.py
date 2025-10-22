from flask import Flask, render_template, request
from flask_login import LoginManager, login_required, current_user
from auth import auth_bp, db, User
from jobs_db import JobDB
from scraper import run_scraper_once, schedule_scraper_in_background
from werkzeug.security import generate_password_hash
import threading

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # replace in production

# Configure SQLAlchemy for users
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
db.init_app(app)
with app.app_context():
    db.create_all()
    # create default admin if none
    if User.query.count() == 0:
        admin = User(username='admin', password=generate_password_hash('admin123', method='pbkdf2:sha256'))
        db.session.add(admin)
        db.session.commit()
        print('Created default admin account -> admin / admin123')

# Login manager
login_manager = LoginManager()
login_manager.login_view = 'auth.login'
login_manager.init_app(app)

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

app.register_blueprint(auth_bp)

# Start scraper scheduler in background thread
def start_scheduler():
    schedule_scraper_in_background(interval_minutes=30, db_path='jobs.db')

t = threading.Thread(target=start_scheduler, daemon=True)
t.start()

@app.route('/')
@login_required
def dashboard():
    q = request.args.get('q','').strip()
    loc = request.args.get('location','').strip()
    jobdb = JobDB('jobs.db')
    jobs = jobdb.recent_jobs(100)
    # filter
    if q:
        jobs = [j for j in jobs if q.lower() in (j.get('title') or '').lower() or q.lower() in (j.get('company') or '').lower()]
    if loc:
        jobs = [j for j in jobs if loc.lower() in (j.get('location') or '').lower()]
    return render_template('dashboard.html', user=current_user, jobs=jobs, q=q, loc=loc)

if __name__ == '__main__':
    # run an initial scrape before starting server
    run_scraper_once(db_path='jobs.db')
    app.run(debug=True)
