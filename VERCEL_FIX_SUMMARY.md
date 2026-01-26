# Vercel Deployment Fix Summary

## Changes Made

### 1. Updated `vercel.json`
- Fixed `installCommand` to use relative paths (`./connectors/...`) instead of `cd` commands
- Added `setuptools` and `wheel` to ensure proper package installation
- Kept Python 3.11 runtime configuration
- Maintained 60-second function timeout

### 2. Removed `package.json`
- Removed `package.json` that might have confused Vercel into thinking this is a Node.js project
- Python serverless functions don't need `package.json`

### 3. Created Troubleshooting Guide
- Added `VERCEL_TROUBLESHOOTING.md` with common issues and solutions

## Current Configuration

**vercel.json:**
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
  },
  "installCommand": "pip install --upgrade pip setuptools wheel && pip install -r requirements.txt && pip install -e ./connectors/tap-postgres && pip install -e ./connectors/tap-mysql && pip install -e ./connectors/tap-mongodb"
}
```

## Next Steps

### 1. Check Vercel Project Settings

Go to your Vercel Dashboard > Project Settings and verify:

- **Framework Preset**: Should be "Other" (not Next.js, not Node.js)
- **Root Directory**: Should be `apps/etl` (if deploying from monorepo root)
- **Build Command**: Should be empty or not set
- **Output Directory**: Should be empty or not set
- **Install Command**: Can be left empty (we're using `vercel.json`)

### 2. Set Environment Variables

In Vercel Dashboard > Settings > Environment Variables, add:

```
CORS_ORIGINS=https://cloud.mantrixflow.com,https://cloud.api.mantrixflow.com,https://cloud.api.etl.server.mantrixflow.com
DATABASE_URL=postgresql://...
SUPABASE_JWT_SECRET=...
LOG_LEVEL=INFO
```

### 3. Ensure Connectors Are Included

Check that `.vercelignore` doesn't exclude the `connectors/` directory:

```bash
# In .vercelignore, make sure connectors/ is NOT listed
# The connectors/ directory must be included in the deployment
```

### 4. Test Locally First

Before deploying, test locally:

```bash
cd apps/etl
vercel dev
```

This will:
- Install dependencies
- Install connectors
- Start local server
- Help catch issues early

### 5. Deploy

Once everything is configured:

```bash
# From apps/etl directory
vercel --prod
```

Or push to your connected Git branch (if auto-deployment is enabled).

## Common Issues

### If "Missing public directory" error persists:

1. **Clear Project Settings:**
   - Go to Vercel Dashboard > Project Settings
   - Clear "Output Directory" if set
   - Clear "Build Command" if set
   - Set Framework Preset to "Other"

2. **Check Root Directory:**
   - If deploying from monorepo root, set Root Directory to `apps/etl`
   - This ensures Vercel looks in the right place

3. **Verify vercel.json:**
   - Make sure `vercel.json` is in `apps/etl/` directory
   - Ensure it has the `builds` configuration

### If installCommand fails:

The installCommand might fail if:
- Connectors have missing dependencies
- Setup.py files have errors
- Paths are incorrect

**Fallback option:** Install connectors at runtime (not recommended for production):

Modify `api/index.py` to install connectors if missing:
```python
import subprocess
import sys
import os

# Install connectors if missing
connectors = ['tap-postgres', 'tap-mysql', 'tap-mongodb']
for connector in connectors:
    try:
        __import__(connector.replace('-', '_'))
    except ImportError:
        connector_path = os.path.join(os.path.dirname(__file__), '..', 'connectors', connector)
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", connector_path])
```

## Verification

After deployment, verify:

1. **Health Check:**
   ```bash
   curl https://your-project.vercel.app/health
   ```

2. **API Docs:**
   ```bash
   curl https://your-project.vercel.app/docs
   ```

3. **Check Logs:**
   - Go to Vercel Dashboard > Deployments > Your Deployment > Functions
   - Check for any import errors or runtime issues

## Support

If issues persist:
1. Check `VERCEL_TROUBLESHOOTING.md` for detailed solutions
2. Review Vercel deployment logs in Dashboard
3. Test with `vercel dev` locally to catch issues early
4. Check Vercel Python runtime documentation
