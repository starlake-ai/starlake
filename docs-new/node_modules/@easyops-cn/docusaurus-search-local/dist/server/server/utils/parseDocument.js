"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.parseDocument = void 0;
const utils_common_1 = require("@docusaurus/utils-common");
const getCondensedText_1 = require("./getCondensedText");
const HEADINGS = "h1, h2, h3";
// const SUB_HEADINGS = "h2, h3";
function parseDocument($) {
    const $pageTitle = $("article h1").first();
    const pageTitle = $pageTitle.text();
    const sections = [];
    const breadcrumb = [];
    const navbarActiveItem = $(".navbar__link--active");
    if (navbarActiveItem.length > 0) {
        breadcrumb.push(navbarActiveItem.eq(0).text().trim());
    }
    const menu = $(".main-wrapper .menu");
    // console.log("menu.length", menu.length);
    if (menu.length > 0) {
        const activeMenuItem = menu
            .eq(0)
            .find(".menu__link--sublist.menu__link--active");
        // console.log("activeMenuItem.length", activeMenuItem.length);
        activeMenuItem.each((_, element) => {
            breadcrumb.push($(element).text().trim());
        });
    }
    $("article")
        .find(HEADINGS)
        .each((_, element) => {
        const $h = $(element);
        // Remove elements that are marked as aria-hidden.
        // This is mainly done to remove anchors like this:
        // <a aria-hidden="true" tabindex="-1" class="hash-link" href="#first-subheader" title="Direct link to heading">#</a>
        const title = $h.contents().not("a.hash-link").text().trim();
        const hash = $h.find("a.hash-link").attr("href") || "";
        // Find all content between h1 and h2/h3,
        // which is considered as the content section of page title.
        let $sectionElements = $([]);
        if ($h.is($pageTitle)) {
            const $header = $h.parent();
            let $firstElement;
            if ($header.is("header")) {
                $firstElement = $header;
            }
            else {
                $firstElement = $h;
            }
            const blogPost = $(`#${utils_common_1.blogPostContainerID}`);
            if (blogPost.length) {
                // Simplify blog post.
                $firstElement = blogPost.children().first();
                $sectionElements = $firstElement.nextUntil(HEADINGS).addBack();
            }
            else {
                const $nextElements = $firstElement.nextAll();
                const $headings = $nextElements.filter(HEADINGS);
                if ($headings.length) {
                    $sectionElements = $firstElement.nextUntil(HEADINGS);
                }
                else {
                    for (const next of $nextElements.get()) {
                        const $heading = $(next).find(HEADINGS);
                        if ($heading.length) {
                            $sectionElements = $sectionElements.add($heading.first().prevAll());
                            break;
                        }
                        else {
                            $sectionElements = $sectionElements.add(next);
                        }
                    }
                }
            }
        }
        else {
            $sectionElements = $h.nextUntil(HEADINGS);
        }
        const content = (0, getCondensedText_1.getCondensedText)($sectionElements.get(), $);
        sections.push({
            title,
            hash,
            content,
        });
    });
    return { pageTitle, sections, breadcrumb };
}
exports.parseDocument = parseDocument;
