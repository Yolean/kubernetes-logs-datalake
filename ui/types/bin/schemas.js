import { readdir } from 'node:fs/promises';
import { mkdir, writeFile } from 'node:fs/promises';
import { z } from 'zod';

const srcDir = new URL('../src/', import.meta.url);
const outDir = new URL('../target/schemas/', import.meta.url);

await mkdir(outDir, { recursive: true });

const files = await readdir(srcDir);
const typeFiles = files.filter((f) => f.endsWith('.type.js'));

for (const file of typeFiles) {
  const mod = await import(new URL(file, srcDir));
  for (const [
    name,
    schema,
  ] of Object.entries(mod)) {
    if (schema instanceof z.ZodType) {
      const jsonSchema = z.toJSONSchema(schema);
      const outFile = new URL(`${name}.schema.json`, outDir);
      await writeFile(outFile, JSON.stringify(jsonSchema, null, 2) + '\n');
      console.log(`${name} -> target/schemas/${name}.schema.json`);
    }
  }
}
