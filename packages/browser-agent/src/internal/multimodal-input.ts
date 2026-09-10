import {
  isSupportedImageType,
  type EncodedBrowserAttachment,
} from "./attachment-store.js";

export function responsesAttachmentPart(
  encoded: EncodedBrowserAttachment,
  requestedName?: string,
) {
  return isSupportedImageType(encoded.metadata.media_type)
    ? { type: "input_image", image_url: encoded.dataUrl, detail: "auto" }
    : {
        type: "input_file",
        filename: requestedName || encoded.metadata.name,
        file_data: encoded.dataUrl,
      };
}

export function messagesAttachmentBlock(
  encoded: EncodedBrowserAttachment,
  requestedName?: string,
) {
  const source = {
    type: "base64",
    media_type: encoded.metadata.media_type,
    data: encoded.base64,
  };
  return isSupportedImageType(encoded.metadata.media_type)
    ? { type: "image", source }
    : {
        type: "document",
        source,
        title: requestedName || encoded.metadata.name,
      };
}
