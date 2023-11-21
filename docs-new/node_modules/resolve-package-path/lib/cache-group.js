'use strict';
const Cache = require("./cache");
module.exports = class CacheGroup {
    constructor() {
        this.MODULE_ENTRY = new Cache();
        this.PATH = new Cache();
        this.REAL_FILE_PATH = new Cache();
        this.REAL_DIRECTORY_PATH = new Cache();
        Object.freeze(this);
    }
};
//# sourceMappingURL=cache-group.js.map