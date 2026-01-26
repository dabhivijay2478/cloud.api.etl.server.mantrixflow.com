# Vercel Deployment Troubleshooting

## Common Issues and Solutions

### Issue: "Missing public directory" Error

This error occurs when Vercel expects a static build output but doesn't find it. For Python serverless functions, this shouldn't happen, but if it does:

**Solution 1: Ensure vercel.json is correct**
- Make sure `vercel.json` has the `builds` configuration pointing to `api/index.py`
- The `routes` should route all traffic to the serverless function
- No `outputDirectory` should be specified (leave it empty or omit it)

**Solution 2: Check Project Settings in Vercel Dashboard**
- Go to Project Settings > General
- Make sure "Framework Preset" is set to "Other" or "Python"
- Clear any "Output Directory" setting
- Clear any "Build Command" that might be set

### Issue: Install Command Failing

If the `installCommand` in `vercel.json` is failing:

**Solution 1: Simplify the install command**
```json
"installCommand": "pip install -r requirements.txt"
```

Then install connectors at runtime (not recommended for production but works for testing).

**Solution 2: Use a build script**
Create a `build.sh` script and reference it:
```json
"installCommand": "bash build.sh"
```

**Solution 3: Install connectors separately**
Install each connector one at a time with error handling:
```json
"installCommand": "pip install -r requirements.txt && (pip install -e connectors/tap-postgres || true) && (pip install -e connectors/tap-mysql || true) && (pip install -e connectors/tap-mongodb || true)"
```

### Issue: Import Errors for Connectors

If you see `ModuleNotFoundError: No module named 'tap_postgres'`:

**Solution 1: Check connector paths**
- Ensure `connectors/` directory is included in deployment
- Check `.vercelignore` doesn't exclude `connectors/`
- Verify `connectors/tap-postgres/setup.py` exists

**Solution 2: Install connectors during build**
Make sure `installCommand` in `vercel.json` installs all connectors:
```json
"installCommand": "pip install -r requirements.txt && pip install -e connectors/tap-postgres && pip install -e connectors/tap-mysql && pip install -e connectors/tap-mongodb"
```

**Solution 3: Runtime installation (fallback)**
Modify `api/index.py` to install connectors at runtime if missing (not recommended for production):
```python
import subprocess
import sys

try:
    import tap_postgres
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-e", "connectors/tap-postgres"])
    import tap_postgres
```

### Issue: Build Timeout

If the build is timing out:

**Solution 1: Increase timeout**
- Vercel has a default build timeout
- For Hobby plan: 45 seconds
- For Pro plan: 5 minutes
- Consider upgrading or optimizing the install process

**Solution 2: Optimize install command**
- Use `--no-cache-dir` for pip to reduce install time
- Install only production dependencies
- Pre-build connectors if possible

### Issue: Function Not Found

If you see "Function not found" errors:

**Solution 1: Check file structure**
- Ensure `api/index.py` exists
- Verify `handler` function is exported
- Check that `main.py` is in the parent directory

**Solution 2: Check routing**
- Verify `routes` in `vercel.json` routes to `api/index.py`
- Ensure the route pattern `"/(.*)"` matches all paths

### Issue: Environment Variables Not Loading

If environment variables aren't available:

**Solution 1: Set in Vercel Dashboard**
- Go to Project Settings > Environment Variables
- Add all required variables
- Redeploy after adding variables

**Solution 2: Check variable names**
- Variable names are case-sensitive
- Use exact names as in your code (`CORS_ORIGINS`, `DATABASE_URL`, etc.)

### Issue: CORS Errors

If you see CORS errors:

**Solution 1: Update CORS origins**
- Set `CORS_ORIGINS` in Vercel environment variables
- Include all allowed origins (comma-separated)
- Redeploy after updating

**Solution 2: Check origin format**
- Use full URLs with protocol: `https://cloud.mantrixflow.com`
- No trailing slashes
- Comma-separated, no spaces (or trimmed in code)

## Deployment Checklist

Before deploying, ensure:

- [ ] `vercel.json` is correctly configured
- [ ] `api/index.py` exists and exports `handler`
- [ ] `requirements.txt` includes all dependencies (including `mangum`)
- [ ] `connectors/` directory is included (not in `.vercelignore`)
- [ ] Environment variables are set in Vercel Dashboard
- [ ] `installCommand` in `vercel.json` installs connectors
- [ ] No `outputDirectory` is set in Project Settings
- [ ] Framework Preset is set to "Other" or "Python"

## Testing Locally

Test the deployment locally before pushing:

```bash
# Install Vercel CLI
npm install -g vercel

# Test locally
cd apps/etl
vercel dev
```

This will simulate the Vercel environment and help catch issues early.

## Getting Help

If issues persist:

1. Check Vercel deployment logs in Dashboard
2. Review function logs for runtime errors
3. Test with `vercel dev` locally
4. Check Vercel documentation: https://vercel.com/docs
5. Review Python runtime docs: https://vercel.com/docs/concepts/functions/serverless-functions/runtimes/python
