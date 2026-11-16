# 展品体验事件湖

本项目使用 FastAPI 和 SQLite 管理场馆、展品、来源契约与事件批次。原始事件保留接收证据，正式报表只读取已发布的日期快照。

## 运行

在预装依赖的环境中，从项目根目录执行：

    uvicorn exhibit_lake.api:app --host 0.0.0.0 --port 8000

默认数据库为本地 SQLite 文件；用 EXHIBIT_LAKE_DB 可指定其他路径。

## 分段入湖

大批次可以拆成按顺序到达的多个段，每段独立落原始行并逐行预校验；
只有整批封存（seal）后，合格行才成为可发布的已接收事件，日期快照不会看到半成品。

    POST /batches/open                      {source_id, contract_id, request_key,
                                             expected_rows?, expected_payload_hash?}
    POST /batches/{batch_id}/segments       {segment_no, events}
    POST /batches/{batch_id}/seal
    POST /batches/{batch_id}/abandon        {reason?}
    GET  /batches/{batch_id}/pending        段进度、挂起行数、已拒收行
    GET  /batches/unfinished[?source_id=]   服务重启后待续或待放弃的批次

- 同一段重放（段号、内容都相同）返回既有结果并标记 `replay: true`；
  段号相同而内容不同返回 `409 conflict`，不会覆盖已落地的行。
- `expected_rows` / `expected_payload_hash` 为整批的声明值，封存时校验；
  不匹配则拒绝封存，批次保持未完成，可继续补段或放弃。
- 未封存或已放弃的批次不会写入 `accepted_events`；`GET /batches/{id}/raw`
  会为每行标注最终归属（`accepted` / `rejected` / `pending` / `abandoned`）。
- 原有的 `POST /batches` 单次接口保持不变，等价于 open + 单段 + seal，
  并与分段流程共享 `(source, request_key)` 的幂等语义。

## 测试与构建

    python -m unittest discover -s tests -v
    python -m compileall -q exhibit_lake
    python -m exhibit_lake.demo

以上三条命令依次运行业务测试、编译检查和本地合成数据验收。依赖版本记录在 requirements.txt。
