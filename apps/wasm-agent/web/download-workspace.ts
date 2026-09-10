import { wasmRuntime } from "./runtime";

export async function downloadWorkspaceFile(path: string) {
  const file = await wasmRuntime.downloadWorkspace(path);
  const url = URL.createObjectURL(new Blob([file.bytes], { type: file.mediaType }));
  const link = document.createElement("a");
  link.href = url;
  link.download = file.name;
  link.style.display = "none";
  document.body.append(link);
  try {
    link.click();
  } finally {
    link.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 0);
  }
  return file;
}
