import yml from "eslint-plugin-yml";

export default [
  ...yml.configs["flat/standard"],
  {
    files: ["**/*.yaml", "**/*.yml"],
    rules: {
      "yml/indent": ["error", 2, { indentBlockSequences: false }],
    },
  },
];
