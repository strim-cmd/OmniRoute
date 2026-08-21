import { HTTP_STATUS } from "@omniroute/open-sse/config/constants.ts";
import {
  releaseQualityClone,
  releaseRejectedQualityResponse,
  validateResponseQuality,
} from "@omniroute/open-sse/services/combo/validateQuality.ts";
import { requiresVisibleTextResponse } from "@omniroute/open-sse/services/responseContract.ts";
import { errorResponse } from "@omniroute/open-sse/utils/error.ts";
import { markTargetCompletedWithoutVisibleContent } from "@/shared/utils/publicFunnelDiagnostics";

type ContractLogger = {
  warn?: (...args: unknown[]) => void;
};

export async function applyDirectVisibleTextContract(
  response: Response,
  fields: {
    isStreaming: boolean;
    requestId: string;
    provider: string;
    model: string;
  },
  log: ContractLogger
): Promise<Response> {
  if (!response.ok || !requiresVisibleTextResponse()) return response;

  let qualityClone: Response;
  try {
    qualityClone = response.clone();
  } catch {
    qualityClone = response;
  }
  const quality = await validateResponseQuality(qualityClone, fields.isStreaming, log);
  releaseQualityClone(qualityClone, response, quality);
  if (quality.valid) return response;

  releaseRejectedQualityResponse(qualityClone, response);
  if (quality.failureCategory === "no_visible_content") {
    markTargetCompletedWithoutVisibleContent(fields.requestId, {
      provider: fields.provider,
      model: fields.model,
      outputKinds: quality.outputKinds,
    });
  }
  return errorResponse(
    HTTP_STATUS.BAD_GATEWAY,
    "Upstream completed without user-visible assistant content"
  );
}
