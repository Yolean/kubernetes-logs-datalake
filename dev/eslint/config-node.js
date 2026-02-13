import js from '@eslint/js';
import globals from 'globals';
import { defineConfig } from 'eslint/config';
import stylistic from '@stylistic/eslint-plugin';
import { stylisticRules } from './rules.js';
import { packageJsonConfig } from './package-json-checks.js';

export default defineConfig([
  js.configs.recommended,
  {
    files: [
      '**/*.js',
    ],
    plugins: {
      '@stylistic': stylistic,
    },
    languageOptions: {
      ecmaVersion: 2022,
      globals: globals.node,
    },
    rules: stylisticRules,
  },
  packageJsonConfig,
]);
