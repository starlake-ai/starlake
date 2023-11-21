import { supportsNodeHttpModule } from "./featureDetection";
import type { RequestFnType } from "./request";
import { requestWithNodeHttpModule } from "./request";

export function getRequesterForNode(): RequestFnType {
  if (supportsNodeHttpModule()) {
    return requestWithNodeHttpModule;
  }

  throw new Error(
    "Could not find a supported HTTP request client in this environment."
  );
}
