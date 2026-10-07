# hq-panel

HQ 判决面板与抵押品质量指数（CQI）的公开数据管线。无需 API 密钥。

## 本地运行

Python 3.11：
```sh
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python cqi.py all
python panel.py all
```

真实输出：`data/cqi_daily.csv`、`data/panel_daily.csv`、
`reports/backtest.md`、`reports/cqi.png`、`reports/panel_latest.md`。
输入来自纽约联储、FRED、TreasuryDirect、美国财政部和 CFTC 公开接口。
抓取错误、空数据或过期数据会令任务失败；不会以旧数据/空报告冒充成功。
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
- 原作者的阈值、权重与研究方法保留；成功运行不代表回测通过或投资结论有效
- 周末、假日、周/月/季频发布有滞后；新鲜度检查按来源容忍合理间隔
- 外部 API 的限流、字段或网址变化会使真实抓取失败，必须查看日志修复，不能绕过错误
- 不要提交密钥、私人 manual 数据或合成演示结果

### 官方程序化入口

CFTC 使用官方 Public Reporting Environment 的 TFF Futures Only API
（数据集 gpe5-46if），按模型所需日期和国债市场筛选、分页并核对总数，
无需每天下载 2016 年起的全部年度 ZIP。该入口由 CFTC 官方明确提供：
https://publicreporting.cftc.gov/stories/s/TFF-Futures-Only/98ig-3k9y/
其官方 FAQ 说明与传统报告使用相同源数据：
https://publicreporting.cftc.gov/stories/s/Public-Reporting-FAQ/inwp-fmhz/

FRED 请求限定模型所需日期（CQI 从 2018-04-02，面板从 2015-01-01），
降低无关历史传输量。每次仍必须取得并验证源数据；不以旧缓存替代失败的刷新。
