import { isRelayType } from "@omniroute/open-sse/utils/proxyDispatcher";

export function shouldUseDispatcherHealthProbe(proxyType: string): boolean {
  return !isRelayType(String(proxyType || "").toLowerCase());
}
