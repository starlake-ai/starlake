import { supportsSendBeacon, supportsXMLHttpRequest } from "./featureDetection";
import type { RequestFnType } from "./request";
import { requestWithSendBeacon, requestWithXMLHttpRequest } from "./request";

export function getRequesterForBrowser(): RequestFnType {
  if (supportsSendBeacon()) {
    return requestWithSendBeacon;
  }

  if (supportsXMLHttpRequest()) {
    return requestWithXMLHttpRequest;
  }

  throw new Error(
    "Could not find a supported HTTP request client in this environment."
  );
}
