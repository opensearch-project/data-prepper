/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

module.exports = {
    root: true,
    parser: '@typescript-eslint/parser',
    ignorePatterns: ['*.d.ts'],
    plugins: [
        '@typescript-eslint',
    ],
    extends: [
        'eslint:recommended',
        'plugin:@typescript-eslint/recommended',
    ],
    rules: {
        "quotes": "off",
        "@typescript-eslint/quotes": ["error", "single"],
        "indent": "off",
        "@typescript-eslint/indent": ["error", 2]
    }
};