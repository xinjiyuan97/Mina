import React from "react";
import { createRoot } from "react-dom/client";

import "@xinjiyuan97/chat-ui/style.css";
import "./styles.css";

import { BrowserWorkbench } from "./browser-workbench";

const root = document.getElementById("root");
if (!root) throw new Error("缺少 React root");

createRoot(root).render(
  <React.StrictMode>
    <BrowserWorkbench />
  </React.StrictMode>,
);
