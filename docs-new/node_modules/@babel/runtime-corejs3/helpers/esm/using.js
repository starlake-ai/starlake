import _typeof from "./typeof.js";
import _Symbol$asyncDispose from "core-js-pure/features/symbol/async-dispose.js";
import _Symbol$for from "core-js-pure/features/symbol/for.js";
import _Symbol$dispose from "core-js-pure/features/symbol/dispose.js";
import _pushInstanceProperty from "core-js-pure/features/instance/push.js";
export default function _using(o, e, n) {
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