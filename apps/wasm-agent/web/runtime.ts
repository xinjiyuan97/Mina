import { BrowserAgent } from "@mina/browser-agent";
export type { RuntimeInfo } from "@mina/browser-agent";
export const agent = new BrowserAgent({ namespace: "mina-browser-agent" });
export const browserAttachmentStore = agent.attachments;
export const wasmRuntime = {
  ready: () => agent.ready(),
  run: agent.debug.runTransitions,
  resolveApproval: agent.resolveApproval.bind(agent),
  generateTitle: agent.generateTitle.bind(agent),
  inspectWorkspace: agent.workspace.inspect,
  downloadWorkspace: agent.workspace.read,
  inspectSkills: agent.skills.list,
  installSkill: agent.skills.install,
};
if (import.meta.hot) import.meta.hot.dispose(() => agent.dispose());
window.addEventListener("pagehide", () => agent.dispose(), { once: true });
