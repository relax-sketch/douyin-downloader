# Douyin Downloader CLI 命令参考

## 快速开始

```bash
# 安装
pip install -e .

# 三种入口
python run.py ...            # 开发用
douyin-dl ...                # pip install 后
python tools/cookie_fetcher.py  # 获取 Cookie
```

---

## 一、下载视频

```bash
douyin-dl -u <URL> [-u <URL> ...] [-c config.yml] [-p ./Downloaded] [-t 5] [-v]
```

| 参数 | 说明 |
|---|---|
| `-u`, `--url` | 抖音链接，可重复多次 |
| `-c`, `--config` | 配置文件路径，默认 `config.yml` |
| `-p`, `--path` | 下载保存目录（覆盖配置文件） |
| `-t`, `--thread` | 并发线程数（覆盖配置文件） |
| `-v`, `--verbose` | 详细日志 |
| `--show-warnings` | 显示 WARNING 级别日志 |
| `--version` | 显示版本号 |

示例：
```bash
python run.py -u https://v.douyin.com/UpQKKQF3LB8/
python run.py -c my_config.yml -t 8 -v
```

---

## 二、分析流水线（pipeline）

**前置：** 需要先下载视频到数据库。

```
douyin-dl pipeline <子命令> [...]
```

### 子命令一览

| 命令 | 说明 | 需要 --run-id |
|---|---|---|
| `run` | 一键完成：准备 + 全阶段执行 | 不需要 |
| `prepare` | 仅创建分析任务，不执行 | 不需要 |
| `continue` | 自动接续最新未完成 run | 不需要 |
| `retry` | 重置失败项并重试 | 可选 |
| `list` | 列出所有 run | 不需要 |
| `resume` | 从断点继续指定 run | **需要** |
| `frames` | 仅执行抽帧 | **需要** |
| `classify` | 仅执行模型判分 | **需要** |
| `export` | 仅导出 CSV | **需要** |
| `organize` | 仅分类归档 | 可选 |

---

### `pipeline run` — 一键全流程

```bash
douyin-dl pipeline run --scope all [--limit N]
douyin-dl pipeline run --urls-file urls.txt
```

| 参数 | 说明 |
|---|---|
| `--scope all` | 处理数据库中所有已下载视频 |
| `--limit N` | 限制处理数量（烟雾测试用） |
| `--urls-file FILE` | 从文件读取 URL，先下载再分析 |

示例：
```bash
# 全量处理
python run.py -c config.yml pipeline run --scope all

# 先烟雾测试 5 条
python run.py -c config.yml pipeline run --scope all --limit 5
```

---

### `pipeline prepare` — 仅创建任务

```bash
douyin-dl pipeline prepare --scope all [--limit N]
```

创建分析 run，把视频加入队列。不执行任何阶段。

```bash
python run.py -c config.yml pipeline prepare --scope all --limit 5
# 输出：Prepared run abc123...: 5 video(s)
```

---

### `pipeline continue` — 自动接续

```bash
douyin-dl pipeline continue
```

自动找到最新的未完成 run（prepared / running / partial），从断点继续。**不需要记 run_id。**

```bash
python run.py -c config.yml pipeline continue
# 输出：Resuming run 4c5c9f... (180/217 classified)
```

---

### `pipeline retry` — 重试失败项

```bash
douyin-dl pipeline retry [--run-id <id>]
```

重置 run 中所有失败项为 pending，保留已成功的阶段数据（不重跑九宫格），然后自动继续。不指定 `--run-id` 时自动选取最新未完成 run。

```bash
# 自动找最新 run 重试
python run.py -c config.yml pipeline retry

# 指定 run
python run.py -c config.yml pipeline retry --run-id abc123...
```

重试逻辑：级联重置

| 失败阶段 | 重置 |
|---|---|
| frames 失败 | frames + classify + export + organize → pending |
| classify 失败 | classify + export + organize → pending |
| export 失败 | export + organize → pending |
| organize 失败 | organize → pending |

---

### `pipeline list` — 查看所有 run

```bash
douyin-dl pipeline list
```

输出示例：
```
4c5c9fead958...  partial     180/217 items  05-18 14:48
bbdb50ed8102...  completed   10/10 items    05-18 15:20
2b42f880c22f...  completed   5/5 items      05-18 13:10
```

---

### `pipeline resume` — 从断点继续指定 run

```bash
douyin-dl pipeline resume --run-id <run_id>
```

从 frames → classify → export → organize 依次执行，跳过已完成阶段。

---

### `pipeline frames / classify / export / organize` — 单阶段执行

```bash
douyin-dl pipeline frames --run-id <run_id>
douyin-dl pipeline classify --run-id <run_id>
douyin-dl pipeline export --run-id <run_id>
douyin-dl pipeline organize [--run-id <run_id>]
```

只执行单个阶段，用于调试或手动控制。

`organize` 可以在模型判分完成后单独运行，只负责按当前配置复制归类，不会重新抽帧或重新请求模型。它会优先使用 `analysis.organize_run_id` 记住的 run；如果这里为空，第一次会自动选择最新已有评分结果的 run，并把 id 写回 `config.yml`：

```bash
# 自动找最新已评分 run
python run.py -c config.yml pipeline organize

# 指定 run
python run.py -c config.yml pipeline organize --run-id <run_id>
```

如果你想切换到另一批评分结果，可以手动改 `config.yml` 里的：

```yaml
analysis:
  organize_run_id: "<run_id>"
```

或者直接带一次 `--run-id`，程序会把新的 id 记住。

如果你改了分桶，想把**已经复制过**的结果按新规则重新归类，用：

```bash
# 自动找最新已评分 run 重建
python run.py -c config.yml pipeline organize --rebuild

# 指定 run 重建
python run.py -c config.yml pipeline organize --run-id <run_id> --rebuild
```

`--rebuild` 会先删除该 run 之前写入 `Classified/` 的副本，再按当前配置重新复制；原始下载视频不会动。

---

## 三、典型工作流

### 第一次使用（从头到尾）

```bash
# 1. 获取 Cookie（仅首次）
python tools/cookie_fetcher.py --config config.yml

# 2. 下载博主所有视频
python run.py -c config.yml

# 3. 一键分析 + 分类
python run.py -c config.yml pipeline run --scope all
```

### 后续增量更新

```bash
# 1. 下载新视频
python run.py -c config.yml

# 2. 分析新视频
python run.py -c config.yml pipeline run --scope all
```

### 中断后继续

```bash
python run.py -c config.yml pipeline continue
```

### 判分失败重试

```bash
python run.py -c config.yml pipeline retry
```

### 烟雾测试（5 条验证配置）

```bash
python run.py -c config.yml pipeline run --scope all --limit 5
```

---

## 四、其他功能

### 热搜榜

```bash
douyin-dl --hot-board      # 全部
douyin-dl --hot-board 20   # 前 20 条
```

输出同名 JSONL 文件。

### 关键词搜索

```bash
douyin-dl --search "关键词" [--search-max 100]
```

### REST API 服务

```bash
douyin-dl --serve [--serve-host 0.0.0.0] [--serve-port 8000]
```

端点：

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/v1/health` | 健康检查 |
| POST | `/api/v1/download` | 提交下载任务 `{"url": "..."}` |
| GET | `/api/v1/jobs/{job_id}` | 查询任务状态 |
| GET | `/api/v1/jobs` | 列出所有任务 |

---

## 五、Cookie 获取工具

```bash
python tools/cookie_fetcher.py [--browser chromium|firefox|webkit] [--output config/cookies.json] [--config config.yml]
```

| 参数 | 说明 |
|---|---|
| `--browser` | 浏览器引擎，默认 chromium |
| `--headless` | 无头模式（不推荐，需手动登录） |
| `--output` | Cookie 输出文件 |
| `--config` | 同时写入配置文件 |
| `--include-all` | 保存所有 Cookie，而非精选子集 |

启动浏览器 → 手动扫码登录 → 终端按回车 → Cookie 自动保存。

---

## 六、Whisper 语音转文字

```bash
python -m cli.whisper_transcribe [-d ./Downloaded] [-m model] [-l zh] [--srt] [--skip-existing]
```

| 参数 | 说明 |
|---|---|
| `-d`, `--dir` | 视频目录 |
| `-f`, `--file` | 单个视频文件 |
| `-m`, `--model` | Whisper 模型：tiny/base/small/medium/large |
| `-l`, `--language` | 语言代码，默认 zh |
| `--srt` | 同时输出 SRT 字幕 |
| `--skip-existing` | 跳过已有转录的视频 |
| `--sc` | 繁体转简体（需 OpenCC） |

---

## 七、配置文件关键项

```yaml
# config.yml 关键配置
analysis:
  enabled: true
  output_dir: ./Analysis/       # 九宫格、CSV 输出
  classified_dir: ./Classified/ # 分类后视频副本
  prompt_file: 提示词.md         # 模型提示词模板
  batch_size: 8                 # 每批送几张九宫格
  allow_partial_batch: true     # 尾批不足时是否继续
  frame_count: 9                # 每个视频抽几帧
  grid_rows: 3                  # 九宫格行数
  grid_cols: 3                  # 九宫格列数
  primary_attribute: suggestiveness_score
  attributes:                   # 评分维度
    - key: suggestiveness_score
      label: 性暗示程度
      min_score: 1
      max_score: 10
    - key: coverage_score
      label: 覆盖程度
      min_score: 1
      max_score: 10
  provider:                     # 多模态模型
    type: openai_compatible
    base_url: https://api.ttk.homes/v1
    model: gemini-3-flash-preview-cli
    timeout: 120
    rate_limit: 1
    retry_times: 3
    image_preprocess:           # 发送前压缩
      enabled: true
      jpeg_quality: 90
      optimize: true
  buckets:                      # 分类分档
    - label: 低
      min_score: 1
      max_score: 3
    - label: 中
      min_score: 4
      max_score: 7
    - label: 高
      min_score: 8
      max_score: 10
```

---

## 八、产物说明

| 产物 | 路径 |
|---|---|
| 原视频 | `Downloaded/<作者>/post/<日期_标题_id>/` |
| 九宫格 | `Analysis/grids/<run_id>/<aweme_id>.jpg` |
| 抽帧 | `Analysis/frames/<run_id>/<aweme_id>/` |
| CSV 结果 | `Analysis/csv/<run_id>.csv` |
| 分类视频 | `Classified/低/` `Classified/中/` `Classified/高/` |
| 数据库 | `dy_downloader.db` |
