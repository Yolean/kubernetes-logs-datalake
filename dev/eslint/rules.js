export const stylisticRules = {
  '@stylistic/indent': [
    'error',
    2,
  ],
  '@stylistic/semi': [
    'error',
    'always',
  ],
  '@stylistic/quotes': [
    'error',
    'single',
    { avoidEscape: true },
  ],
  '@stylistic/comma-dangle': [
    'error',
    'always-multiline',
  ],
  '@stylistic/eol-last': [
    'error',
    'always',
  ],
  '@stylistic/no-trailing-spaces': 'error',
  '@stylistic/no-multiple-empty-lines': [
    'error',
    { max: 1, maxEOF: 0 },
  ],
  '@stylistic/object-curly-spacing': [
    'error',
    'always',
  ],
  '@stylistic/array-bracket-spacing': [
    'error',
    'never',
  ],
  '@stylistic/arrow-parens': [
    'error',
    'always',
  ],
  '@stylistic/brace-style': [
    'error',
    '1tbs',
  ],
  '@stylistic/comma-spacing': [
    'error',
    { before: false, after: true },
  ],
  '@stylistic/key-spacing': [
    'error',
    { beforeColon: false, afterColon: true },
  ],
  '@stylistic/keyword-spacing': [
    'error',
    { before: true, after: true },
  ],
  '@stylistic/space-before-blocks': [
    'error',
    'always',
  ],
  '@stylistic/space-before-function-paren': [
    'error',
    { anonymous: 'always', named: 'never', asyncArrow: 'always' },
  ],
  '@stylistic/space-infix-ops': 'error',
  '@stylistic/array-element-newline': [
    'error',
    { multiline: true, minItems: 1 },
  ],
  '@stylistic/array-bracket-newline': [
    'error',
    { multiline: true, minItems: 1 },
  ],
};
