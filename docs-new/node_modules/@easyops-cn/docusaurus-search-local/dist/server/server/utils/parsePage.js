"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parsePage = void 0;
const debug_1 = require("./debug");
const getCondensedText_1 = require("./getCondensedText");
function parsePage($, url) {
    $("a[aria-hidden=true]").remove();
    let $pageTitle = $("h1").first();
    if ($pageTitle.length === 0) {
        $pageTitle = $("title");
    }
    const pageTitle = $pageTitle.text();
    const $main = $("main");
    if ($main.length === 0) {
        (0, debug_1.debugWarn)("page has no <main>, therefore no content was indexed for this page %o", url);
    }
    return {
        pageTitle,
        sections: [
            {
                title: pageTitle,
                hash: "",
                content: $main.length > 0 ? (0, getCondensedText_1.getCondensedText)($main.get(0), $).trim() : "",
            },
        ],
        breadcrumb: [],
    };
}
exports.parsePage = parsePage;
