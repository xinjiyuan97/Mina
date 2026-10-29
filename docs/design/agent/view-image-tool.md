# Agent 主动读图：view_image

`view_image` 让模型主动读取工作区内的图片，并在下一次模型调用中获得真实视觉输入。

```json
{"path":"screenshots/result.png"}
```

路径相对于宿主配置的工作区。支持 PNG、JPEG、GIF、WebP；格式按文件内容识别，不依赖扩展名。限制为 16 MiB、单边 8192 像素、总计 16 megapixels。会读取头信息并进行受内存限制的解码校验；损坏文件、超限图片、越界路径和受保护文件会返回工具错误。沿用 WorkspaceFs 的路径和符号链接限制，不接受远程 URL。动画图片保留原文件；实际帧展示由模型供应商决定。

## 数据流

1. ViewImageTool 从 WorkspaceFs 读取图片，验证后保存到 BlobStore。
2. ToolOutput 返回可读元数据（路径、格式、宽高、字节数）和 ModelAttachment 引用，事件中不内嵌图片 base64。
3. Tools 算子将附件引用写入 `gate.result.v1`，Gate 收齐结果后传给 Context。
4. Context 先追加该批所有 tool results，再追加带图片附件的 user 消息，注明来源工具和 call ID。这样保持工具调用配对，并兼容只接受 user 图片的模型接口。
5. Provider 从同一个 BlobStore 读取图片，在发给模型时转换为相应图片输入格式。

当前图状态及内部事件保留 blob 引用，可恢复后续轮次。图片正文不等于工具文本输出，也不自动作为面向用户的聊天图片展示。新会话轮次需要保留相关 ModelMessage，或由 agent 再次读图。

## 装配

工具实现：`agent_extension::tool::ViewImageTool`。单独注册时传入 WorkspaceFs 和 BlobStore。

内置目录可通过共享 BlobStore 启用：

```rust,ignore
let provider = ConfiguredModelProvider::from_model_config(model)?
    .with_blob_store(blobs.clone());
let tools = BuiltinToolCatalog::new(workspace)
    .with_blob_store(blobs)
    .build()?;
```

Server、CLI 和配置了 workspace 的 agent_chat_demo 已接入。CLI 与 Server 使用 `MINA_BLOB_STORE_PATH`，默认 `data/blobs`。没有 BlobStore 的自定义 BuiltinToolCatalog 不会宣告 view_image，以免返回模型无法解析的图片引用。宿主工具白名单仍可禁用它。

模型必须支持图片输入。若图启用了 Compactor，还需设置 `attachment_tokens` 为每张图片的保守 token 预算；不要仅按图片引用文本长度估算。Compactor 会保留带附件的消息，预算不足时仍按既有失败策略处理。

## 验证

ViewImageTool 测试覆盖四种格式的真实字节存取、错误扩展名、损坏图片、字节/像素上限、路径与符号链接越界、取消，以及并行读图后模型消息顺序和恢复不重复执行。现有 Provider 附件测试验证图片输入编码。
