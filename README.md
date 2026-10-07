# hq-panel

HQ 判决面板与抵押品质量指数（CQI）的公开数据管线。FRED 数据使用官方 API，需要用户自己的 FRED_API_KEY。

## 本地运行

Python 3.12（与验证环境一致）。先通过本地安全方式设置 FRED_API_KEY 环境变量；不要把密钥写入代码、命令行历史或日志：
```sh
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python cqi.py all
python panel.py all
```

真实输出：`data/cqi_daily.csv`、`data/panel_daily.csv`、
`reports/backtest.md`、`reports/cqi.png`、`reports/panel_latest.md`。
输入来自纽约联储、FRED、TreasuryDirect、美国财政部和 CFTC 公开接口。
核心来源（SOFR、准备金利率、信任背离的两个 FRED 序列）抓取错误、空数据或过期时任务失败。可选来源失败时明确标注降级，对应成分排除，至少仍须有 3 个有效成分；不会以旧数据/空报告冒充成功。
报告保留原始观测日期，填充日期不代表源数据更新。

`python cqi.py demo` 和 `python panel.py demo` 只用于离线检查，输出隔离在
`data/demo/`，不能当作真实报告或有效回测结果。

## 自动运行

保留原定时配置：UTC 周一至周五 23:30（北京时间次日 07:30）。
可在 Actions → HQ Panel Daily → Run workflow 手动运行。
PR 运行单元测试和真实抓取/生成，**不写回仓库**。
合并后，默认分支的定时/手动任务仅在全部生成成功时提交 data/ 与 reports/。
工作流已经按任务设置 contents: write；不要为测试扩大账号或仓库权限。
若默认分支保护禁止机器人推送，发布会明确失败，需另行选择批准的发布方式。

## 数据质量与研究边界

- manual/ 可缺省：TIC、COFER、估值等未提供时显示“待接入”或省略，不造数
- 按此次 V0.3 稿加入第六成分“信任背离”：10 年期收益率 5 日上行且美元指数 5 日下跌时取收益率上行基点数；其余阈值及合成方式保留
- 成功运行不代表回测通过或投资结论有效；降级报告不能作为正式研究关口依据
- 周末、假日、周/月/季频发布有滞后；新鲜度检查按来源容忍合理间隔
- 外部 API 的限流、字段或网址变化会使真实抓取失败，必须查看日志修复；核心失败停发，可选失败按上述规则明确降级
- 不要提交密钥、私人 manual 数据或合成演示结果

### 官方程序化入口

CFTC 使用官方 Public Reporting Environment 的 TFF Futures Only API
（数据集 gpe5-46if），按模型所需日期和国债市场筛选、分页并核对总数，
无需每天下载 2016 年起的全部年度 ZIP。该入口由 CFTC 官方明确提供：
https://publicreporting.cftc.gov/stories/s/TFF-Futures-Only/98ig-3k9y/
其官方 FAQ 说明与传统报告使用相同源数据：
https://publicreporting.cftc.gov/stories/s/Public-Reporting-FAQ/inwp-fmhz/

FRED 使用官方 observations API（JSON、原生频率与水平值），限定模型所需日期（CQI 从 2018-04-02，面板从 2015-01-01），
降低无关历史传输量。每次仍必须取得并验证源数据；不以旧缓存替代失败的刷新。


## FRED 密钥设置（由用户本人操作）

1. 在 FRED 官方页面登录并为本项目申请自己的 API key：
   https://fredaccount.stlouisfed.org/apikeys
2. 在本 GitHub 仓库依次打开 Settings → Secrets and variables → Actions → Secrets → New repository secret
3. Name 填 FRED_API_KEY，Secret 由用户本人粘贴密钥，点击 Add secret
4. 完成后重新运行本 PR 的失败作业进行真实数据验证；不要发送密钥或含密钥的截图

密钥仅以环境变量提供给需要 FRED 的步骤，不提供给单元测试、安装依赖或提交报告步骤。
程序只请求固定的官方 HTTPS API，不打印请求 URL/密钥/服务端错误正文，不跟随重定向。
缺少密钥会立即失败并说明配置未完成；单元测试通过并不代表真实 API 校验通过。
fork PR 与 Dependabot 事件通常拿不到仓库密钥，不能把这种未执行的真实校验视为通过。
不回退到旧 CSV 入口，不以历史缓存掩盖失败。

官方说明：
- FRED API key：https://fred.stlouisfed.org/docs/api/api_key.html
- FRED observations：https://fred.stlouisfed.org/docs/api/fred/series_observations.html
- GitHub 仓库 secrets：https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-secrets#creating-secrets-for-a-repository
