# GitHub Pages Setup Guide

## Step 1: Create GitHub Repository

1. Go to GitHub and create a new repository named `mosheb.github.io`
2. **Important**: Create it under your username `mosheb`
3. Make it public (required for GitHub Pages)
4. Don't initialize with README (we already have one)

## Step 2: Push Your Code

```bash
# Commit all files
git commit -m "Initial commit: Portfolio site with Astro"

# Add your GitHub repository as remote
git remote add origin https://github.com/mosheb/mosheb.github.io.git

# Push to GitHub
git push -u origin main
```

## Step 3: Enable GitHub Pages

1. Go to your repository on GitHub
2. Click **Settings** (top menu)
3. In the left sidebar, click **Pages**
4. Under **Source**, select **GitHub Actions**
5. That's it! The workflow will automatically deploy your site

## Step 4: Wait for Deployment

1. Go to the **Actions** tab in your repository
2. You should see a workflow running called "Deploy to GitHub Pages"
3. Wait for it to complete (usually takes 1-2 minutes)
4. Once done, your site will be live at: `https://mosheb.github.io/mosheb.github.io`

## Customizing Your Site

### Update Personal Information

Edit these files to customize:

1. **src/layouts/BaseLayout.astro** - Change site title and navigation
2. **src/pages/index.astro** - Update your name and introduction
3. **src/pages/about.astro** - Add your bio and technologies
4. **astro.config.mjs** - Already configured for your username

### Add New Projects

Create a new markdown file in `src/content/projects/`:

```bash
# Create a new project file
touch src/content/projects/my-new-project.md
```

Then edit it with this template:

```markdown
---
title: 'Your Project Name'
description: 'A brief description of your project'
pubDate: 2026-07-01
tags: ['TypeScript', 'React', 'Node.js']
status: 'completed'  # or 'in-progress' or 'archived'
repo: 'https://github.com/mosheb/your-repo'
demo: 'https://your-demo-url.com'  # optional
---

## Overview

Write about your project here...

## Features

- Feature 1
- Feature 2

## Technical Stack

What technologies did you use?
```

### Test Locally

```bash
# Install dependencies (if not already done)
npm install

# Start development server
npm run dev

# Open http://localhost:4321 in your browser

# Build for production (test)
npm run build
```

### Deploy Updates

Every time you push to the main branch, GitHub Actions will automatically rebuild and deploy your site:

```bash
git add .
git commit -m "Add new project"
git push
```

Wait 1-2 minutes and your changes will be live!

## Troubleshooting

### Site Not Loading?

1. Check the Actions tab for failed workflows
2. Make sure GitHub Pages is enabled in Settings → Pages
3. Verify the repository is public
4. Check that the repository name matches what's in `astro.config.mjs`

### Need to Change the Base URL?

If you rename the repository, update `astro.config.mjs`:

```javascript
export default defineConfig({
  site: 'https://mosheb.github.io',
  base: '/new-repo-name',  // Change this
  // ...
});
```

## What's Included

- ✅ Dark theme design
- ✅ Responsive layout
- ✅ Project showcase
- ✅ About page
- ✅ RSS feed
- ✅ Automatic deployment
- ✅ TypeScript support
- ✅ MDX for rich content

## Next Steps

1. Remove the sample project in `src/content/projects/sample-project.md`
2. Add your real projects
3. Update the About page with your information
4. Customize colors in `src/styles/global.css`
5. Add your own favicon in `public/favicon.svg`

Enjoy your new portfolio site! 🚀
