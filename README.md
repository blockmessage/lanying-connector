# 蓝莺连接器 Lanying Connector

将蓝莺IM与其他服务连接起来，可以接收来自蓝莺IM回调服务的消息，与被连接服务如AI引擎交互，并可以将后者的回复发回到蓝莺IM。

蓝莺IM，是由[美信拓扑](https://www.lanyingim.com/)团队研发的新一代即时通讯云服务，SDK设计简单集成方便，服务采用云原生技术和多云架构，私有云也可按月付费。

当前已有模板（欢迎补充&PR）:

1. openai：通过调用[OpenAI API](https://beta.openai.com)来实现一个ChatGPT Chatbot。
2. openai-xiaolan：蓝莺IM中的小蓝AI设置，仅为演示智能客服功能用；

### 系统要求

[Python 3.7](https://www.python.org/downloads/)

### 安装与运行

1. 克隆本工程并进入工程目录
   ```bash
   $ cd lanying-connector
   ```

2. 激活虚拟环境

   ```bash
   $ python3 -m venv venv
   $ . venv/bin/activate
   ```

3. 安装依赖

   ```bash
   $ pip install -r requirements.txt
   ```

4. 复制环境变量模板文件，并进行配置

   ```bash
   $ cp .env.example .env
   ```
   其中：
   
   ```LANYING_USER_ID``` 是提供Chatbot服务的用户ID；
   
   ```LANYING_ADMIN_TOKEN``` 是蓝莺IM[管理员Token](https://console.lanyingim.com/#/home/token);
   
   ```LANYING_CONNECTOR_SERVICE``` 选择交互引擎，这里默认是 openai;
   
   ```LANYING_API_ENDPOINT``` 仅私有云需要，是应用所在API服务的地址，可从蓝莺IM控制台"应用信息"页面获取;

   ```LANYING_CONNECTOR_REDIS_SERVER``` redis的地址， 格式如：redis://:@redis:6379/0

5. 配置服务
   
   如果```LANYING_CONNECTOR_SERVICE```选择了 openai，就对应修改 configs/openai.json 对其进行配置,
   具体配置可参照[OpenAI文档](https://beta.openai.com/docs/api-reference/authentication)。

6. 运行

   ```bash
   $ flask run
   ```
   注：每次重新运行需要激活虚拟环境，别忘了操作第2步。

### Chat Completions 回放验证（新旧协议）

用于回放 `/v1/chat/completions` 的典型场景（旧 `functions/function_call`、新 `tools/tool_calls`、`stream`、`content` list）：

```bash
# 查看内置用例
python3 scripts/replay_chat_completions.py --list

# 执行全部用例
LANYING_CONNECTOR_BASE_URL=http://127.0.0.1:5000 \
LANYING_CONNECTOR_API_KEY='YOUR_BEARER_TOKEN' \
LANYING_CONNECTOR_MODEL='gpt-4o-mini' \
python3 scripts/replay_chat_completions.py

# 只跑单个用例
python3 scripts/replay_chat_completions.py --api-key 'YOUR_BEARER_TOKEN' --only new_tools_stream_forced_tool
```

### 灰度联调脚本与验收模板

```bash
# 1) 拷贝一份灰度用例模板（按你的业务场景改造）
cp scripts/gray_replay_cases.template.json scripts/gray_replay_cases.json

# 2) 执行灰度联调（自动产出日志到 scripts/reports/）
LANYING_CONNECTOR_BASE_URL='http://127.0.0.1:5000' \
LANYING_CONNECTOR_API_KEY='YOUR_BEARER_TOKEN' \
LANYING_CONNECTOR_MODEL='gpt-4o-mini' \
LANYING_CONNECTOR_GRAY_CASES='scripts/gray_replay_cases.json' \
./scripts/run_gray_validation.sh
```

- 日志文件：`scripts/reports/gray_validation_YYYYMMDD_HHMMSS.log`
- 验收记录模板：`scripts/gray_acceptance_template.md`
- 建议每个厂商灰度批次单独留一份日志与模板记录（便于回溯）

### 可上线检查清单（Model API 升级）

- 协议兼容：
  - 新请求格式 `tools/tool_choice` 可正常调用；
  - 旧请求格式 `functions/function_call` 仍可调用；
  - `messages[].content` 支持 `str | list`（至少验证 text + image_url 组合）。
- 响应契约：
  - 非流式优先返回 `message.tool_calls`，并使用 `finish_reason=tool_calls`；
  - 流式 `delta.tool_calls[*].function.arguments` 能正确增量拼接；
  - 多 `tool_calls` 在同一轮可顺序执行并继续对话。
- 历史兼容：
  - 旧历史中的 `assistant.function_call` / `role=function` 可被运行时兼容加载；
  - 新历史写入为 `assistant.tool_calls` / `role=tool` 语义。
- 厂商适配：
  - 目标厂商至少覆盖一次工具调用场景（baidu/minimax/azure/claude/aws + openai-like）；
  - 工具调用关闭模型不会错误透传 `tools/tool_choice`。
- 观测与统计：
  - token 统计与配额扣减在 `content=list` 下无异常；
  - 错误日志中能区分厂商响应异常与协议转换异常。

### 回归命令清单

```bash
# 1) 协议转换与桥接单测
python3 -m unittest tests/test_openai_compat.py
python3 -m unittest tests/test_vendor_protocol_contract.py

# 2) 全量 tests 目录回归
python3 -m unittest discover -s tests -p 'test_*.py'

# 3) 关键文件语法检查
python3 -m py_compile \
  services/openai_service.py \
  lanying_openai_compat.py \
  lanying_vendor.py \
  lanying_vendor_openai.py \
  lanying_vendor_aliyun.py \
  lanying_vendor_moonshot.py \
  lanying_vendor_volcengine.py \
  lanying_vendor_siliconflow.py \
  lanying_vendor_zhipuai.py \
  lanying_vendor_deepseek.py \
  lanying_vendor_azure.py \
  lanying_vendor_azure2.py \
  lanying_vendor_baidu.py \
  lanying_vendor_minimax.py \
  lanying_vendor_claude.py \
  lanying_vendor_aws.py

# 4) 端到端回放（需服务与 token）
LANYING_CONNECTOR_BASE_URL=http://127.0.0.1:5000 \
LANYING_CONNECTOR_API_KEY='YOUR_BEARER_TOKEN' \
LANYING_CONNECTOR_MODEL='gpt-4o-mini' \
python3 scripts/replay_chat_completions.py
```

服务启动成功，就可以在页面上看到收发消息的基本情况了：[http://127.0.0.1:5000](http://127.0.0.1:5000)，祝玩得开心~🚀
