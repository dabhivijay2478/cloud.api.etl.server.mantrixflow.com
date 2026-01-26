# Vercel Deployment Guide for Python ETL Service

This guide explains how to deploy the FastAPI ETL microservice to Vercel using Python serverless functions.

## Prerequisites

1. **Vercel Account**: Sign up at [vercel.com](https://vercel.com)
2. **Vercel CLI**: Install globally
   ```bash
   npm install -g vercel
   ```
3. **Python 3.11**: Vercel uses Python 3.11 by default (configured in `vercel.json`)

## Project Structure

```
apps/etl/
├── api/
│   └── index.py          # Vercel serverless entrypoint
├── connectors/           # Singer taps (tap-postgres, tap-mysql, tap-mongodb)
├── main.py              # FastAPI application
├── transformer.py       # Custom transformation logic
├── utils.py             # Utility functions
├── requirements.txt     # Python dependencies (includes Mangum)
├── vercel.json          # Vercel configuration
└── .env.local.example   # Environment variables template
```

## Local Development Setup

### 1. Install Dependencies

```bash
cd apps/etl

# Install Python dependencies
pip install -r requirements.txt

# Install Singer taps (local connectors)
pip install -e connectors/tap-postgres
pip install -e connectors/tap-mysql
pip install -e connectors/tap-mongodb
```

### 2. Configure Environment Variables

```bash
# Copy example environment file
cp .env.local.example .env.local

# Edit .env.local with your actual values
# For local development, you can use .env instead
cp .env.example .env
```

### 3. Test Locally with Vercel Dev

```bash
# From apps/etl directory
vercel dev
```

This will:
- Start a local Vercel development server
- Simulate the serverless environment
- Hot-reload on code changes
- Use environment variables from `.env.local`

The API will be available at `http://localhost:3000`

## Deployment Steps

### Step 1: Login to Vercel

```bash
vercel login
```

### Step 2: Link Your Project (First Time)

```bash
cd apps/etl
vercel link
```

This will:
- Prompt you to select/create a Vercel project
- Create `.vercel` directory with project configuration

### Step 3: Set Environment Variables

Set environment variables in Vercel Dashboard or via CLI:

**Via Dashboard:**
1. Go to your project on [vercel.com](https://vercel.com)
2. Navigate to **Settings** > **Environment Variables**
3. Add each variable from `.env.local.example`

**Via CLI:**
```bash
# Set environment variables for production
vercel env add SUPABASE_JWT_SECRET production
vercel env add DATABASE_URL production
vercel env add LOG_LEVEL production

# Set for preview environments too
vercel env add SUPABASE_JWT_SECRET preview
vercel env add DATABASE_URL preview
```

### Step 4: Deploy to Production

```bash
# Deploy to production
vercel --prod
```

Or deploy to preview:
```bash
# Deploy to preview (staging)
vercel
```

### Step 5: Verify Deployment

After deployment, Vercel will provide:
- Production URL: `https://your-project.vercel.app`
- API endpoints: `https://your-project.vercel.app/api/...`

Test the health endpoint:
```bash
curl https://your-project.vercel.app/health
```

## Configuration Files

### vercel.json

```json
{
  "version": 2,
  "builds": [
    {
      "src": "api/index.py",
      "use": "@vercel/python"
    }
  ],
  "routes": [
    {
      "src": "/(.*)",
      "dest": "api/index.py"
    }
  ],
  "env": {
    "PYTHON_VERSION": "3.11"
  },
  "functions": {
    "api/index.py": {
      "maxDuration": 60
    }
  }
}
```

**Key Settings:**
- `@vercel/python`: Uses Vercel's Python runtime
- `PYTHON_VERSION`: Set to 3.11 (compatible with psycopg2-binary)
- `maxDuration`: 60 seconds (maximum execution time for serverless functions)

### api/index.py

This is the Vercel serverless entrypoint that:
- Imports the FastAPI app from `main.py`
- Wraps it with Mangum (ASGI-to-serverless adapter)
- Exports a `handler` function for Vercel

## Environment Variables

Required environment variables (set in Vercel Dashboard):

| Variable | Description | Example |
|----------|-------------|---------|
| `SUPABASE_JWT_SECRET` | JWT secret for Supabase auth | `your-secret-key` |
| `DATABASE_URL` | PostgreSQL connection string | `postgresql://user:pass@host:5432/db` |
| `LOG_LEVEL` | Logging level | `INFO` or `DEBUG` |
| `MONGODB_URL` | MongoDB connection (optional) | `mongodb://host:27017/db` |
| `MYSQL_URL` | MySQL connection (optional) | `mysql://user:pass@host:3306/db` |

## Build Settings in Vercel Dashboard

If deploying via Dashboard (not CLI), configure:

1. **Framework Preset**: Other
2. **Root Directory**: `apps/etl`
3. **Build Command**: (leave empty - Vercel auto-detects Python)
4. **Output Directory**: (leave empty)
5. **Install Command**: `pip install -r requirements.txt && pip install -e connectors/tap-postgres && pip install -e connectors/tap-mysql && pip install -e connectors/tap-mongodb`

**Note**: For Singer taps, you may need to install them during build. Consider:
- Adding a `build.sh` script
- Or including tap dependencies in `requirements.txt` if possible

## Troubleshooting

### Issue: Import Errors for Connectors

**Problem**: `ModuleNotFoundError: No module named 'tap_postgres'`

**Solution**: The Singer taps need to be installed. Options:
1. Install during build (add to build command)
2. Package taps as proper Python packages
3. Use a custom build script

### Issue: Timeout Errors

**Problem**: Functions timing out after 10 seconds

**Solution**: Increase `maxDuration` in `vercel.json`:
```json
"functions": {
  "api/index.py": {
    "maxDuration": 60
  }
}
```

### Issue: Environment Variables Not Loading

**Problem**: `os.getenv()` returns `None`

**Solution**: 
- Ensure variables are set in Vercel Dashboard
- Redeploy after adding variables
- Check variable names match exactly (case-sensitive)

### Issue: CORS Errors

**Problem**: CORS errors from frontend

**Solution**: Update CORS origins in `main.py`:
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://your-frontend.vercel.app"],  # Add your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

## API Endpoints

After deployment, all FastAPI endpoints are available:

- `GET /` - Service status
- `GET /health` - Health check
- `POST /discover-schema/{sourceType}` - Discover schema
- `POST /collect/{sourceType}` - Collect data
- `POST /transform` - Transform records
- `POST /emit/{destType}` - Emit data
- `POST /delta-check/{sourceType}` - Incremental sync check
- `GET /docs` - Swagger UI documentation
- `GET /openapi.json` - OpenAPI schema

## Monitoring

- **Vercel Dashboard**: View function logs, metrics, and errors
- **Function Logs**: Available in Vercel Dashboard > Functions tab
- **Analytics**: Enable in Vercel Dashboard > Analytics

## Continuous Deployment

Vercel automatically deploys when you push to your connected Git branch:
- `main` branch → Production
- Other branches → Preview deployments

## Next Steps

1. Set up environment variables in Vercel Dashboard
2. Deploy: `vercel --prod`
3. Test endpoints using the provided URL
4. Monitor logs in Vercel Dashboard
5. Configure custom domain (optional)

## Additional Resources

- [Vercel Python Documentation](https://vercel.com/docs/concepts/functions/serverless-functions/runtimes/python)
- [Mangum Documentation](https://mangum.io/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
