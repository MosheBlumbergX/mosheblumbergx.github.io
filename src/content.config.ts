import { defineCollection, z } from 'astro:content';
import { glob } from 'astro/loaders';

const projects = defineCollection({
  loader: glob({ pattern: '**/*.{md,mdx}', base: './src/content/projects' }),
  schema: z.object({
    title: z.string(),
    description: z.string(),
    pubDate: z.coerce.date(),
    updatedDate: z.coerce.date().optional(),
    tags: z.array(z.string()).default([]),
    status: z.enum(['in-progress', 'completed', 'archived']).default('in-progress'),
    repo: z.string().url().optional(),
    demo: z.string().url().optional(),
    image: z.string().optional(),
  }),
});

export const collections = { projects };
