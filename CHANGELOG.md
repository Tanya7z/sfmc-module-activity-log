## 0.2.4

- 修复 `flush()` 异常时由于立即重试可能导致的死循环与 CPU 飙高风险，加入失败冷却退避。
- 引入有界队列容量控制与防 OOM 淘汰策略 (`max_queue_size`)。
- 修复 SAPI `isValid` 属性校验逻辑，消除类型警告与潜在的空指针异常。
- 修复 `entityItemDrop` 掉落物实际物品 ID 丢失（误记为 `minecraft:item`）的问题。
- 修复 `entityDie` 杀人与死亡事件分离，确保 PvP 场景双方完整入库，避免死者坐标覆盖击杀者坐标。
- 修复 `world.explosion` 爆炸事件过于严苛的来源过滤（覆盖 TNT/苦力怕/水晶等常见爆炸）。
- 优化 `activity.query` 分页查询性能，杜绝全表扫描。
- 补齐单元测试并修复 ESLint 检查配置。

## 0.2.2

- 更新模块 Schema 引用及展示信息。
