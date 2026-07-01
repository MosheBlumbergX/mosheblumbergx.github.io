# Moshe Blumberg's Portfolio

Personal portfolio site built with Astro and deployed to GitHub Pages.

## 🚀 Built With

- [Astro](https://astro.build/) - Modern static site generator
- TypeScript - Type-safe development
- MDX - Markdown with JSX for project content
- CSS - Custom styling with CSS variables

## 🏗️ Structure

```
src/
├── content/
│   └── projects/        # Project markdown files
├── layouts/             # Page layouts
│   ├── BaseLayout.astro
│   └── ProjectLayout.astro
├── components/          # Reusable components
│   └── ProjectCard.astro
├── pages/              # Site pages
│   ├── index.astro
│   ├── about.astro
│   ├── projects/
│   │   ├── index.astro
│   │   └── [...slug].astro
│   └── rss.xml.ts
└── styles/
    └── global.css      # Global styles
```

## 💻 Development

```bash
# Install dependencies
npm install

# Start dev server
npm run dev

# Build for production
npm run build

# Preview production build
npm run preview
```

## 📝 Adding Projects

Create a new `.md` or `.mdx` file in `src/content/projects/`:

```markdown
---
title: 'Project Title'
description: 'Brief description'
pubDate: 2026-07-01
tags: ['TypeScript', 'React']
status: 'completed'
repo: 'https://github.com/username/repo'
demo: 'https://demo-url.com'
---

Your project content here...
```

## 🚀 Deployment

The site automatically deploys to GitHub Pages when you push to the main branch.

### Setup GitHub Pages

1. Go to your repository settings
2. Navigate to Pages section
3. Set Source to "GitHub Actions"
4. Push to main branch to trigger deployment

Your site will be available at: `https://mosheblumbergx.github.io`

## 📄 License

MIT License - feel free to use this as a template for your own portfolio!
