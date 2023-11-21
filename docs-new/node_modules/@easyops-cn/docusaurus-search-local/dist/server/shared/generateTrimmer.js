"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.generateTrimmer = void 0;
function generateTrimmer(wordCharacters) {
    const startRegex = new RegExp("^[^" + wordCharacters + "]+", "u");
    const endRegex = new RegExp("[^" + wordCharacters + "]+$", "u");
    return function (token) {
        return token.update(function (str) {
            return str.replace(startRegex, "").replace(endRegex, "");
        });
    };
}
exports.generateTrimmer = generateTrimmer;
