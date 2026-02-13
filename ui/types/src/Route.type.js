import { z } from 'zod';

export const Route = z.object({
  name: z.string()
    .min(3)
    .max(8)
    .regex(/^[a-z][a-z0-9-]*[a-z0-9]$/, 'Must be valid in kubernetes deployment names'),
});
