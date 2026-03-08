# MCP 操作工具详细使用指南（Codex + loco_explorer）

更新时间：2026-03-08  
适用用户：`kkk`  
适用项目：任意项目（全局配置已启用）

---

## 1. 这份文档解决什么问题

这份指南告诉你：

1. 如何确认全局 MCP 工具已生效。  
2. 在对话里如何下指令，稳定触发 MCP。  
3. 每个工具该在什么时候用。  
4. 如何用最省 token 的方式完成“检索 -> 阅读 -> 修改”。  
5. 常见报错怎么排查。

---

## 2. 全局配置位置（已落地）

当前全局配置与服务位置：

- Codex 全局配置：`/home/kkk/.codex/config.toml`
- MCP 服务脚本：`/home/kkk/loco-mcp/loco_mcp_server.py`
- 虚拟环境：`/home/kkk/loco-mcp/.venv`
- 远端模型地址：`http://192.168.1.3:11434`
- 模型名：`frob/locooperator:latest`

你在任何项目目录启动 Codex 都会复用这套 MCP。

---

## 3. 先做可用性检查（每次新会话建议执行）

在终端执行：

```bash
codex mcp list
```

期望看到：

- `loco_explorer` 存在
- `enabled` 为 true

再检查远程 Ollama：

```bash
curl http://192.168.1.3:11434/api/version
curl http://192.168.1.3:11434/api/tags
```

---

## 4. 你实际会用到的 MCP 工具名

`loco_explorer` 当前暴露四个核心工具：

1. `list_project_files`
2. `search_code`
3. `read_file_excerpt`
4. `summarize_for_change`

> 你通常不需要手动写参数 JSON。  
> 直接用自然语言告诉 Codex“先用哪个工具做什么”即可。

---

## 5. 每个工具怎么用（作用 + 场景）

## 5.1 `list_project_files`

作用：快速看某个目录有哪些文件（不读全文）。  
适合：刚开始定位改动范围。

示例指令（对 Codex 说）：

```text
先调用 list_project_files 看 core、services、web_console 的候选文件。
```

---

## 5.2 `search_code`

作用：在候选目录里按关键字定位命中行。  
适合：找函数、字段、API 路由、配置项。

示例指令：

```text
用 search_code 查 strategy_tier、tier_guarded、/api/events 在项目中的位置。
```

---

## 5.3 `read_file_excerpt`

作用：只读取某文件某个行号区间。  
适合：拿到精确上下文后再改代码。

示例指令：

```text
只用 read_file_excerpt 读取命中文件的关键区间（每段不超过 200 行）。
```

---

## 5.4 `summarize_for_change`

作用：把 3-8 个候选文件压缩成结构化改动建议。  
适合：复杂任务开工前做变更影响总结。

示例指令：

```text
先用 summarize_for_change 总结这 5 个文件的改动影响，再开始改代码。
```

---

## 6. 推荐工作流（强烈建议）

按这个顺序最稳、最省 token：

1. `list_project_files` 缩小目录范围
2. `search_code` 找关键命中
3. `read_file_excerpt` 读取局部片段
4. （可选）`summarize_for_change` 得到改动计划
5. Codex 开始修改
6. 测试与回归验证

---

## 7. 你在对话里该怎么说（可直接复制）

## 模板 A：先分析不改代码

```text
先不要改代码。优先使用 MCP（list_project_files -> search_code -> read_file_excerpt）。
先给我 3-8 个最相关文件和最小改动方案。
```

## 模板 B：边分析边修改

```text
优先使用 MCP 控制 token。先检索并局部读取，再直接修改并跑测试。
输出修改文件清单、测试结果和风险点。
```

## 模板 C：强制先用工具

```text
必须先调用 loco_explorer 的 list_project_files、search_code、read_file_excerpt，
不要直接全文件遍历。
```

---

## 8. 在“另一个项目”里如何确认也能发现工具

进入任意项目目录后执行：

```bash
cd /path/to/another-project
codex mcp list
```

如果出现 `loco_explorer enabled`，说明全局可用。  
然后在 Codex 会话里输入：

```text
/mcp
```

可以看到当前已加载工具列表。

---

## 9. 常见问题排查

## 9.1 `codex mcp list` 看不到 `loco_explorer`

检查：

- `/home/kkk/.codex/config.toml` 是否存在并含 `mcp_servers.loco_explorer`
- 启动 Codex 的用户是否是 `kkk`

## 9.2 MCP 可见但调用失败

先测脚本环境：

```bash
/home/kkk/loco-mcp/.venv/bin/python -m py_compile /home/kkk/loco-mcp/loco_mcp_server.py
```

再测 Ollama 连通：

```bash
curl http://192.168.1.3:11434/api/version
```

## 9.3 搜索时报权限错误

已在 MCP 里对 `.beads`、`.git` 等目录做排除与容错。  
若新目录仍报权限问题，补充排除 glob 即可。

---

## 10. Token 控制准则（执行时必须遵守）

1. 先检索再阅读，不先读大文件。  
2. 每次只读局部片段，不灌全文件。  
3. 一轮候选文件控制在 3-8 个。  
4. 必要时先做 `summarize_for_change` 再改。  
5. 改动后只补读受影响文件，不回头全仓扫描。

---

## 11. 和后续任务的衔接建议

针对 `Upgrade10`，后续建议固定使用：

1. 先用 MCP 做 `T3/T4` 相关文件定位与片段读取。  
2. 再做页面实时轮询与事件时间线改造。  
3. 改完立即跑 `pytest` 与 `web_console` 的 typecheck。  
4. 输出 DoD 对照和剩余风险。

