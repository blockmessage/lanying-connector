# 灰度联调验收模板（Chat Completions）

## 1. 基本信息
- 验收日期：
- 验收环境（staging/prod-gray）：
- 网关地址（LANYING_CONNECTOR_BASE_URL）：
- 模型（LANYING_CONNECTOR_MODEL）：
- 厂商配置：
- 操作人：

## 2. 用例输入
- 用例文件：
- 是否包含旧协议（functions/function_call）：是 / 否
- 是否包含新协议（tools/tool_choice）：是 / 否
- 是否包含流式 tool_calls：是 / 否
- 是否包含 content list（text/image_url）：是 / 否

## 3. 执行命令
```bash
LANYING_CONNECTOR_BASE_URL='http://127.0.0.1:5000' \
LANYING_CONNECTOR_API_KEY='YOUR_BEARER_TOKEN' \
LANYING_CONNECTOR_MODEL='gpt-4o-mini' \
LANYING_CONNECTOR_GRAY_CASES='scripts/gray_replay_cases.json' \
./scripts/run_gray_validation.sh
```

## 4. 结果记录
- 日志路径：
- 用例统计（pass/fail/total）：
- 失败用例列表：
- 失败原因摘要：

## 5. 契约核对（关键项）
- [ ] 非流式工具调用返回 `message.tool_calls`
- [ ] 工具调用结束 `finish_reason=tool_calls`
- [ ] 流式 `delta.tool_calls[*].function.arguments` 正常增量拼接
- [ ] 旧协议输入仍可执行并映射为新语义
- [ ] 多段 `content`（list）不报错，统计/日志无异常

## 6. 结论
- 验收结论：通过 / 有条件通过 / 不通过
- 阻塞项：
- 后续动作与负责人：
