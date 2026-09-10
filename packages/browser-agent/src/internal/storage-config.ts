// Worker-local configuration: set once during initialization, never by a run.
let directory = "mina-browser-agent";
let configured = false;
export function validateNamespace(value: string) {
  if (!/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,95}$/.test(value)) {
    throw new Error("namespace must be 1–96 letters, digits, underscores or hyphens");
  }
  return value;
}
export function configureStorage(namespace: string) {
  if (configured) throw new Error("Worker storage already initialized");
  directory = validateNamespace(namespace);
  configured = true;
}
export function storageDirectory() { return directory; }
export function workspacePrefix() { return `${directory}/files`; }
export function workspaceUrl() { return `opfs://${workspacePrefix()}`; }
export function skillsPrefix() { return `${directory}/skills`; }
export function skillsUrl() { return `opfs://${skillsPrefix()}`; }
