# Quick Start: Vercel Deployment

## Files Created

✅ **vercel.json** - Vercel configuration for Python serverless functions  
✅ **api/index.py** - Vercel serverless entrypoint (wraps FastAPI with Mangum)  
✅ **requirements.txt** - Updated with Mangum dependency  
✅ **.env.local.example** - Environment variables template  
✅ **.vercelignore** - Files to exclude from deployment  
✅ **.gitignore** - Updated with Vercel-specific entries  
✅ **build.sh** - Build script for Singer taps installation  
✅ **VERCEL_DEPLOYMENT.md** - Complete deployment guide  

## Quick Deployment

```bash
# 1. Install Vercel CLI (if not already installed)
npm install -g vercel

# 2. Navigate to ETL directory
cd apps/etl

# 3. Login to Vercel
vercel login

# 4. Link project (first time only)
vercel link

# 5. Set environment variables (via Dashboard or CLI)
# Go to: https://vercel.com/dashboard > Your Project > Settings > Environment Variables
# Or use: vercel env add VARIABLE_NAME production

# 6. Deploy to production
vercel --prod
```

## Required Environment Variables

Set these in Vercel Dashboard > Settings > Environment Variables:

- `SUPABASE_JWT_SECRET` - JWT secret for Supabase authentication
- `DATABASE_URL` - PostgreSQL connection string
- `LOG_LEVEL` - Logging level (default: INFO)
- `MONGODB_URL` - (Optional) MongoDB connection string
- `MYSQL_URL` - (Optional) MySQL connection string

## Test Locally

```bash
# Test with Vercel dev server
vercel dev

# API will be available at http://localhost:3000
```

## API Endpoints

After deployment, access:
- Health: `https://your-project.vercel.app/health`
- Docs: `https://your-project.vercel.app/docs`
- All FastAPI endpoints are available at the root URL

## Troubleshooting

**Import errors for connectors?**  
The `vercel.json` includes an `installCommand` that installs Singer taps. If issues persist, check build logs in Vercel Dashboard.

**Timeout errors?**  
Increase `maxDuration` in `vercel.json` (currently 60 seconds).

**Environment variables not working?**  
- Ensure variables are set in Vercel Dashboard
- Redeploy after adding variables
- Check variable names are exact (case-sensitive)

For detailed instructions, see [VERCEL_DEPLOYMENT.md](./VERCEL_DEPLOYMENT.md)
