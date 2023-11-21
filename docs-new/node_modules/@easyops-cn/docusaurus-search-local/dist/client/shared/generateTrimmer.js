export function generateTrimmer(wordCharacters) {
    const startRegex = new RegExp("^[^" + wordCharacters + "]+", "u");
    const endRegex = new RegExp("[^" + wordCharacters + "]+$", "u");
    return function (token) {
        return token.update(function (str) {
            return str.replace(startRegex, "").replace(endRegex, "");
        });
    };
}
