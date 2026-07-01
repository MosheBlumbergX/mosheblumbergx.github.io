# Quick Deployment Guide

## ✅ Everything is configured for `mosheb.github.io`

Your portfolio is ready to deploy! Here's what to do:

## 1. Commit Your Code

```bash
git commit -m "Initial commit: Portfolio site with Astro"
```

## 2. Push to GitHub

```bash
# Add your GitHub repository as remote
git remote add origin https://github.com/mosheb/mosheb.github.io.git

# Push to GitHub
git push -u origin main
```

## 3. Enable GitHub Pages

1. Go to https://github.com/mosheb/mosheb.github.io/settings/pages
2. Under **Source**, select **GitHub Actions**
3. Done! Your site will deploy automatically

## 4. View Your Site

After 1-2 minutes, your site will be live at:

**https://mosheb.github.io**

(Note: Since you're using `username.github.io`, there's no `/project-name` at the end!)

## What's Configured

✅ `astro.config.mjs` - Set to `https://mosheb.github.io` (no base path needed)
✅ `SETUP.md` - All instructions updated for your repository
✅ `README.md` - Correct deployment URL
✅ GitHub Actions workflow - Ready to deploy
✅ Sample project links - Using `mosheb` username

## User vs Project Pages

Your repository `mosheb.github.io` is a **user GitHub Pages site**, which means:
- ✅ No base path needed (cleaner URLs)
- ✅ Deploys to the root domain `https://mosheb.github.io`
- ✅ Simpler configuration

If you had used a different repo name like `portfolio`, it would be a project site at `https://mosheb.github.io/portfolio`.

## Next Steps After Deployment

1. Verify the site is live at https://mosheb.github.io
2. Remove the sample project: `rm src/content/projects/sample-project.md`
3. Add your real projects
4. Update `src/pages/about.astro` with your information
5. Customize as needed!

## Test Locally First (Optional)

```bash
npm run dev
# Visit http://localhost:4321
```

Everything is ready to go! 🚀
