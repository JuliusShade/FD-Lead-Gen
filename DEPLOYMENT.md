# Deployment Guide for Heroku

## Option 1: Two Separate Apps (Recommended)

### Backend API Deployment

1. **Install production dependencies**:
```bash
cd backend
pip install gunicorn
pip freeze > requirements.txt
```

2. **Files created**:
- `Procfile`: Tells Heroku how to run the app
- `runtime.txt`: Specifies Python version

3. **Deploy to Heroku**:
```bash
cd backend
heroku create fd-leadgen-api
heroku config:set DB_HOST=fd-lead-platform.cqjooueks11d.us-east-1.rds.amazonaws.com
heroku config:set DB_PORT=5432
heroku config:set DB_DATABASE=postgres
heroku config:set DB_USERNAME=postgres
heroku config:set "DB_PASSWORD=(95>?bCJBp9b~)uhBZZs.mKlc9q("
heroku config:set DB_SSL=true
git init
git add .
git commit -m "Deploy backend API"
git push heroku main
```

4. **Test the API**:
```bash
curl https://fd-leadgen-api.herokuapp.com/api/health
curl https://fd-leadgen-api.herokuapp.com/api/jobs/summary?limit=5
```

### Frontend Deployment

1. **Create `.env.production` in frontend/**:
```
VITE_API_URL=https://fd-leadgen-api.herokuapp.com
```

2. **Create `static.json` in frontend/** (for serving static files):
```json
{
  "root": "dist",
  "clean_urls": true,
  "routes": {
    "/**": "index.html"
  },
  "headers": {
    "/**": {
      "Cache-Control": "no-cache, no-store, must-revalidate"
    },
    "/assets/**": {
      "Cache-Control": "public, max-age=31536000, immutable"
    }
  }
}
```

3. **Update `package.json` to add heroku-postbuild script**:
```json
{
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview",
    "start": "vite preview",
    "heroku-postbuild": "npm run build"
  }
}
```

4. **Deploy to Heroku**:
```bash
cd frontend
heroku create fd-leadgen-app
heroku buildpacks:add heroku/nodejs
heroku buildpacks:add https://github.com/heroku/heroku-buildpack-static
heroku config:set VITE_API_URL=https://fd-leadgen-api.herokuapp.com
git init
git add .
git commit -m "Deploy frontend"
git push heroku main
```

5. **Open your app**:
```bash
heroku open
```

---

## Option 2: Single App (Backend serves Frontend)

Update `api_server.py` to also serve the built frontend:

```python
from flask import Flask, jsonify, request, send_from_directory
import os

app = Flask(__name__, static_folder='../frontend/dist', static_url_path='')

# Serve frontend
@app.route('/')
def serve_frontend():
    return send_from_directory(app.static_folder, 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    if path.startswith('api/'):
        return jsonify({'error': 'Not found'}), 404

    file_path = os.path.join(app.static_folder, path)
    if os.path.exists(file_path):
        return send_from_directory(app.static_folder, path)
    return send_from_directory(app.static_folder, 'index.html')

# ... rest of API routes
```

Then deploy as single app:
```bash
cd backend
git init
git add .
git commit -m "Combined deployment"
heroku create fd-leadgen-combined
heroku config:set [all DB configs]
git push heroku main
```

---

## Environment Variables Summary

### Backend (.env / Heroku Config)
```
DB_HOST=fd-lead-platform.cqjooueks11d.us-east-1.rds.amazonaws.com
DB_PORT=5432
DB_DATABASE=postgres
DB_USERNAME=postgres
DB_PASSWORD=(95>?bCJBp9b~)uhBZZs.mKlc9q(
DB_SSL=true
PORT=5001  # Heroku sets this automatically
```

### Frontend (.env.production)
```
VITE_API_URL=https://fd-leadgen-api.herokuapp.com
```

---

## Post-Deployment Testing

1. **Test backend API**:
```bash
curl https://fd-leadgen-api.herokuapp.com/api/health
# Should return: {"status":"ok","service":"FD Lead Gen API"}

curl https://fd-leadgen-api.herokuapp.com/api/jobs/summary?limit=2
# Should return JSON with job data
```

2. **Test frontend**:
- Open https://fd-leadgen-app.herokuapp.com
- Should see login page
- Click login → should see dashboard
- Expand "Job Posting Leads" → should see qualified jobs table

3. **Check logs**:
```bash
# Backend logs
heroku logs --tail --app fd-leadgen-api

# Frontend logs
heroku logs --tail --app fd-leadgen-app
```

---

## Important Notes

1. **Database Access**: Your Aurora RDS must allow connections from Heroku IPs
   - Go to AWS RDS → Security Groups
   - Add inbound rule for PostgreSQL (port 5432) from `0.0.0.0/0` (or Heroku IP ranges)

2. **Free Tier Limitations**:
   - Heroku free tier apps sleep after 30 min of inactivity
   - First request after sleep takes 10-30 seconds
   - Consider upgrading to Hobby tier ($7/month per app) for always-on

3. **CORS**: Already configured with `CORS(app)` in api_server.py ✅

4. **Database Connection Pooling**: For production, consider using connection pooling:
```python
from psycopg2 import pool
db_pool = pool.SimpleConnectionPool(1, 20, **db_config)
```

---

## Recommended Deployment Strategy

**For MVP/Testing**: Use Option 2 (single app) for simplicity

**For Production**: Use Option 1 (two apps) for better scalability:
- Frontend can be cached by CDN
- Backend can scale independently
- Easier to update each separately
