import rss from '@astrojs/rss';
import { getCollection } from 'astro:content';
import type { APIContext } from 'astro';

export async function GET(context: APIContext) {
  const projects = await getCollection('projects');
  return rss({
    title: 'Moshe Blumberg - Projects',
    description: 'Software projects and experiments',
    site: context.site || 'https://mosheb.github.io',
    items: projects.map(project => ({
      title: project.data.title,
      description: project.data.description,
      pubDate: project.data.pubDate,
      link: `/projects/${project.id.replace(/\.mdx?$/, '')}/`,
    })),
  });
}
