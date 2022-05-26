module.exports = {
    extends: 'eslint:recommended',
    env : {
        node : true,
        es6 : true,
        mocha : true,
    },
    plugins: [
        'promise',
        'mocha',
    ],
    rules : {
        'no-console' : 'off',
        'indent': [ 'error', 4 ],
        "mocha/no-exclusive-tests": "error",
    },
};
