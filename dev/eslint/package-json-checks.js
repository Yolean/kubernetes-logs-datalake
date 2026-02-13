import jsoncParser from 'jsonc-eslint-parser';

const plugin = {
  meta: { name: 'package-json-checks' },
  rules: {
    'require-type-module': {
      meta: {
        type: 'problem',
        messages: {
          missing: 'package.json must have "type": "module"',
        },
      },
      create(context) {
        return {
          'JSONExpressionStatement > JSONObjectExpression'(node) {
            const typeProp = node.properties.find(
              (p) => p.key.type === 'JSONLiteral' && p.key.value === 'type',
            );
            if (!typeProp || typeProp.value.value !== 'module') {
              context.report({
                node: typeProp || node,
                messageId: 'missing',
              });
            }
          },
        };
      },
    },
    'require-private': {
      meta: {
        type: 'problem',
        messages: {
          missing: 'package.json must have "private": true',
        },
      },
      create(context) {
        return {
          'JSONExpressionStatement > JSONObjectExpression'(node) {
            const privateProp = node.properties.find(
              (p) => p.key.type === 'JSONLiteral' && p.key.value === 'private',
            );
            if (!privateProp || privateProp.value.value !== true) {
              context.report({
                node: privateProp || node,
                messageId: 'missing',
              });
            }
          },
        };
      },
    },
    'require-version-zero': {
      meta: {
        type: 'problem',
        messages: {
          missing: 'package.json must have "version": "0.0.0"',
        },
      },
      create(context) {
        return {
          'JSONExpressionStatement > JSONObjectExpression'(node) {
            const versionProp = node.properties.find(
              (p) => p.key.type === 'JSONLiteral' && p.key.value === 'version',
            );
            if (!versionProp || versionProp.value.value !== '0.0.0') {
              context.report({
                node: versionProp || node,
                messageId: 'missing',
              });
            }
          },
        };
      },
    },
  },
};

export const packageJsonConfig = {
  files: [
    '**/package.json',
  ],
  plugins: {
    'package-json-checks': plugin,
  },
  languageOptions: {
    parser: jsoncParser,
  },
  rules: {
    'package-json-checks/require-type-module': 'error',
    'package-json-checks/require-private': 'error',
    'package-json-checks/require-version-zero': 'error',
  },
};
