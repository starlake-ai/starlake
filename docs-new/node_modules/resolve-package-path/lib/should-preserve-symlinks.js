'use strict';
function includes(array, entry) {
    for (let i = 0; i < array.length; i++) {
        if (array[i] === entry) {
            return true;
        }
    }
    return false;
}
module.exports = function (process) {
    return !!process.env.NODE_PRESERVE_SYMLINKS || includes(process.execArgv, '--preserve-symlinks');
};
//# sourceMappingURL=should-preserve-symlinks.js.map