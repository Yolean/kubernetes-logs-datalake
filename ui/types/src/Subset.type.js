import { z } from 'zod';

export const Subset = z.object({
  cluster: z.string(),
  namespace: z.string().optional(),
  // todo: pod
  // todo: container
  // todo: range
});
