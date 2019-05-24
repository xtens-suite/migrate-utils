module.exports = {
    "env": {
        "browser": true,
        "node": true,
        "es6": true
    },
    "extends": ["eslint:recommended", "plugin:backbone/recommended"],
    "parserOptions": {
        "ecmaFeatures": {
            "experimentalObjectRestSpread": true,
            "jsx": true
        },
        "sourceType": "module"
    },
    "globals": {
        "_": true,
        "$": true,
        "JST": true,
        "d3": true,
        "xtens": true,
        "Backbone": true

    },
    "plugins": ["backbone"],
    "rules": {
        "indent": ["error",4],
        "linebreak-style": [
            "error",
            "unix"
        ],
        "quotes": [0],
        "semi": [
            "warn",
            "always"
        ],
        "no-console": [0],
        "no-unused-vars": [0],
        "no-regex-spaces": [0]
    }
};
