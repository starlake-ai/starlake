var _Symbol$asyncDispose = require("core-js-pure/features/symbol/async-dispose.js");
var _Symbol$for = require("core-js-pure/features/symbol/for.js");
var _Symbol$dispose = require("core-js-pure/features/symbol/dispose.js");
var _pushInstanceProperty = require("core-js-pure/features/instance/push.js");
var _typeof = require("./typeof.js")["default"];
function _using(o, e, n) {
  if (null == e) return e;
  if ("object" != _typeof(e)) throw new TypeError("using declarations can only be used with objects, null, or undefined.");
  if (n) var r = e[_Symbol$asyncDispose || _Symbol$for("Symbol.asyncDispose")];
  if (null == r && (r = e[_Symbol$dispose || _Symbol$for("Symbol.dispose")]), "function" != typeof r) throw new TypeError("Property [Symbol.dispose] is not a function.");
  return _pushInstanceProperty(o).call(o, {
    v: e,
    d: r,
    a: n
  }), e;
}
module.exports = _using, module.exports.__esModule = true, module.exports["default"] = module.exports;