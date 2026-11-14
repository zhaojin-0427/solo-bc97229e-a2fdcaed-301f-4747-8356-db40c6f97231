# Seq Lake — 双端 FASTQ 成对入湖服务

测序仪把 R1、R2 两个 gzip FASTQ **分开发送**，本服务负责**确认成对完整后再放行**给下游比对程序：

1. 建批 → 2. 分端流式上传（顺序不限）→ 3. 两端各自落盘并校验 → 4. 成对一致性确认后批次置 `READY`，两端原件**一起**开放下载，并返回读段数。

## 快速开始

### Docker Compose（交付方式）

```bash
docker compose up --build
```

服务监听 `8000` 端口，数据持久化在命名卷 `seq-data`（SQLite 库 + 原件）。

交互式 API 文档（Swagger UI）：<http://localhost:8000/docs>
可直接在页面上演示 **建批 → 上传（文件选择器）→ 查询 → 下载** 全流程。

### 本地开发

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
DATA_DIR=./data .venv/bin/python -m uvicorn app.main:app --port 8000   # 单 worker
```

### 演示与测试

```bash
bash scripts/demo.sh                 #  happy 路径：建批/上传/查询/下载并逐字节比对
python3 scripts/smoke_test.py --base http://127.0.0.1:8000 --data-dir ./data
                                     # 43 项端到端断言（含并发、断连、慢上传中断、非阻塞）
python3 scripts/make_sample.py /tmp/demo --reads 1000   # 生成样例，可加 --bad-qual 等制造坏件
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| `POST` | `/batches` | 建批，返回 `batch_id`（状态 `PENDING`） |
| `POST` | `/batches/{id}/ends/{R1\|R2}/upload` | 流式上传一端（请求体为 `application/gzip` 原始字节）。同批同端**仅受理一次** |
| `GET` | `/batches/{id}` | 批次状态、两端进度、读段数、失败原因 |
| `GET` | `/batches?status=` | 批次列表（可按状态过滤） |
| `GET` | `/batches/{id}/ends/{R1\|R2}/download` | 下载原件（仅批次 `READY` 后两端同时开放） |

批次状态机：`PENDING → READY`（两端均校验通过且成对一致）/ `PENDING → FAILED`（任一端接收或校验失败、或服务重启）。

上传响应示例（第二端完成时）：

```json
{
  "batch_id": "…", "end": "R2", "end_status": "VALIDATED",
  "batch_status": "READY", "records": 1000,
  "size_bytes": 181483, "sha256": "…", "read_count": 1000
}
```

## 校验规则（流式、边收边验）

- **gzip 完整**：流式解压，缺尾（截断）、CRC 错误、流尾垃圾均判失败；
- **限四行式记录**：`@` 头行、序列行、`+` 行、质量行，缺一不可（含文件末尾残记录）；
- **序列与质量等长**；
- **读段名配对**：取 `@` 后首个空白前的文本，去掉末尾 `/1` 或 `/2`；两端**逐条相等且条数一致**；
- 空文件（0 条记录）判失败。

## 关键语义与实现要点

- **同端仅一次、并发不覆盖**：`(batch_id, end)` 为数据库主键，受理即插入 `RECEIVING` 行，并发同端只有一个获胜（另一个 `409`）；原件发布用硬链接 `link(2)`，物理上无法覆盖。
- **一端失败，另一端立即中断**：每批次有内存态 `fail_event`；在途上传的每次读流都与该事件**竞速**，事件一置位立刻中止，**不等慢正文收完**。失败方等双方退出、残件（临时件与已落盘件）清除后才把批次置 `FAILED` 并响应；迟到收尾在落库前会被状态条件拦截，不得放行。
- **不阻塞他人**：接收循环全异步，落盘/解压/校验经 `asyncio.to_thread` 移出事件循环；数据库只有微秒级短事务（WAL 模式），慢上传与校验期间其他批次和查询不受影响。
- **内存不随文件大小增长**：固定大小分块（1 MiB）流式处理；读段名不落地，折叠进**有序 SHA-256 哈希链**（`H=SHA256(每条: 长度‖名字)`），两端比对最终摘要+条数即等价于逐条相等；单行长度设上限（默认 16 MiB）。实测 254 MB / 150 万条上传，进程 RSS 波动 < 1 MB。
- **重启恢复**：启动时把所有非终态批次标 `FAILED`（原因 `service restarted…`）并清除其残件与孤儿临时件；`READY` 批次及原件原样保留，重启后仍可下载。
- **单 worker 运行**：双端协调在进程内存中（重启即失败恢复，无需持久），因此 uvicorn 以 `--workers 1` 运行（Compose 中已固定）。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `/data`（容器）/ `./data`（本地） | SQLite 库与原件根目录 |
| `MAX_LINE_BYTES` | `16777216` | 单行上限，超出判失败 |
| `PEER_DRAIN_TIMEOUT` | `60` | 失败方等待对端退出清理的秒数 |

## 目录结构

```
app/
  main.py      # FastAPI 端点、双端失败传播、启动恢复、OpenAPI 定制
  fastq.py     # 流式 gzip + 四行 FASTQ 校验器（O(1) 内存）
  db.py        # SQLite（WAL、短事务、条件状态机转移）
  runtime.py   # 批次内存协调态（fail_event / all_clear）
  storage.py   # 目录布局、原子发布（硬链接）、残件清理
  config.py    # 环境变量配置
scripts/
  demo.sh          # happy 路径演示
  smoke_test.py    # 43 项端到端测试（仅标准库）
  make_sample.py   # 样例/坏件生成器
Dockerfile  docker-compose.yml  requirements.txt
```

数据目录：`lake.db`（SQLite）、`tmp/`（在途 `.part` 临时件）、`batches/<batch_id>/{R1,R2}.fastq.gz`（校验通过后的原件）。
