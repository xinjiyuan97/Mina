import type { ModelMessage } from "./protocol.js";

export class ResponsesInputError extends Error {}

export function responsesInput(messages: ModelMessage[]) {
  return messages.flatMap((message) => {
    if (message.role === "tool") {
      if (!message.tool_call_id) {
        throw new ResponsesInputError("工具结果缺少 tool_call_id");
      }
      return [
        { type: "function_call_output", call_id: message.tool_call_id, output: message.content },
      ];
    }

    const items: unknown[] = [];
    if (message.content) {
      items.push({
        role: message.role,
        content: [
          {
            type: message.role === "assistant" ? "output_text" : "input_text",
            text: message.content,
          },
        ],
      });
    }
    for (const call of message.tool_calls ?? []) {
      items.push({
        type: "function_call",
        call_id: call.id,
        name: call.name,
        arguments: call.arguments,
      });
    }
    return items;
  });
}

export function responsesContinuationInput(messages: ModelMessage[]) {
  const { toolResults } = splitResponsesContinuationMessages(messages);
  return responsesInput(toolResults);
}

export function responsesStatelessContinuationInput(
  history: unknown[],
  messages: ModelMessage[],
) {
  return [...history, ...responsesContinuationInput(messages)];
}

export function splitResponsesContinuationMessages(messages: ModelMessage[]) {
  let lastAssistantIndex = -1;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    if (
      messages[index]?.role === "assistant" &&
      (messages[index]?.tool_calls?.length ?? 0) > 0
    ) {
      lastAssistantIndex = index;
      break;
    }
  }
  const toolResults = messages
    .slice(lastAssistantIndex + 1)
    .filter((message) => message.role === "tool");
  if (lastAssistantIndex < 0 || toolResults.length === 0) {
    throw new ResponsesInputError("Responses continuation 缺少新的工具结果");
  }
  return {
    prefix: messages.slice(0, lastAssistantIndex),
    toolResults,
  };
}
