-- ============================================================
-- SQL 面试高频题练习（电商场景）
-- ============================================================
-- 用法：
--   1. 先执行下面「建表 + 造数据」整段，把测试数据灌进 MySQL
--   2. 每道题在「你的答案」下面写 SQL
--   3. 写完自己跑一遍验证，再让我批改
--
-- 覆盖题型：
--   Q1 分组聚合 + HAVING
--   Q2 窗口函数：每组 Top N（面试最高频！）
--   Q3 窗口函数：第 N 高
--   Q4 连续登录 N 天（大厂经典）
--   Q5 同比/环比：LAG 对比上一期
-- ============================================================


-- ============================================================
-- 建表 + 造数据（直接整段执行）
-- ============================================================
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS user_login;

-- 订单表
CREATE TABLE orders (
    order_id     INT PRIMARY KEY,
    user_id      INT,
    category     VARCHAR(20),   -- 商品品类
    amount       DECIMAL(10,2), -- 订单金额
    order_date   DATE
);

INSERT INTO orders VALUES
(1,  101, '电子', 5000, '2025-01-05'),
(2,  102, '电子', 3000, '2025-01-06'),
(3,  103, '服装', 800,  '2025-01-06'),
(4,  101, '服装', 1200, '2025-01-07'),
(5,  104, '食品', 200,  '2025-01-07'),
(6,  102, '电子', 4500, '2025-02-03'),
(7,  105, '食品', 150,  '2025-02-04'),
(8,  103, '电子', 6000, '2025-02-10'),
(9,  101, '食品', 300,  '2025-02-11'),
(10, 104, '服装', 2000, '2025-02-15'),
(11, 105, '电子', 3500, '2025-03-01'),
(12, 102, '服装', 900,  '2025-03-02'),
(13, 103, '食品', 250,  '2025-03-05'),
(14, 101, '电子', 7000, '2025-03-08'),
(15, 104, '电子', 2800, '2025-03-10');

-- 用户登录表
CREATE TABLE user_login (
    user_id     INT,
    login_date  DATE
);

INSERT INTO user_login VALUES
(101, '2025-01-01'),
(101, '2025-01-02'),
(101, '2025-01-03'),  -- 101 连续登录 1/1~1/3
(101, '2025-01-05'),
(102, '2025-01-01'),
(102, '2025-01-03'),  -- 102 没连续
(103, '2025-01-01'),
(103, '2025-01-02'),
(103, '2025-01-03'),
(103, '2025-01-04'),  -- 103 连续登录 1/1~1/4
(104, '2025-01-10');


-- ============================================================
-- Q1【分组聚合 + HAVING】
-- 统计每个品类的：订单数、总金额、平均金额。
-- 只保留「总金额 > 8000」的品类，按总金额降序。
-- ------------------------------------------------------------
-- 考点：GROUP BY / COUNT / SUM / AVG / HAVING / ORDER BY
-- 你的答案：
SELECT
    category,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount,
    AVG(amount) AS avg_amount
FROM orders
GROUP BY category
HAVING SUM(amount) > 8000
ORDER BY SUM(amount) DESC



-- ============================================================
-- Q2【窗口函数 - 每组 Top N】★面试最高频
-- 找出每个品类中，金额最高的前 2 笔订单。
-- 输出：category, order_id, amount
-- ------------------------------------------------------------
-- 考点：ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
-- 提示：先用窗口函数编号，再在外层筛 rn <= 2
-- 你的答案：
SELECT
    category,
    order_id,
    amount
FROM 
    (SELECT
        category,
        order_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rn
    FROM orders) AS ranked_orders
WHERE rn <= 2




-- ============================================================
-- Q3【窗口函数 - 第 N 高】
-- 查询订单金额「第 3 高」的订单（金额并列算同一名次）。
-- 输出：order_id, amount
-- ------------------------------------------------------------
-- 考点：DENSE_RANK()（并列名次用它，不是 ROW_NUMBER）
-- 想想：为什么这里用 DENSE_RANK 而不是 RANK 或 ROW_NUMBER？
-- 你的答案：
SELECT
    order_id,
    amount
FROM (
    SELECT
        category,
        order_id,
        amount,
        DENSE_RANK() OVER (ORDER BY amount DESC) AS rnk
    FROM orders
) AS ranked_orders
WHERE rnk = 3



-- ============================================================
-- Q4【连续登录 N 天】★大厂经典
-- 找出「连续登录 3 天及以上」的用户。
-- 输出：user_id
-- ------------------------------------------------------------
-- 考点：经典技巧 —— 登录日期 减去 行号，连续的日期会得到相同的"基准日"
--       row_number 按 user 分区、按日期排序
--       login_date - INTERVAL row_number DAY  → 连续段落得到同一个值
--       再 GROUP BY 那个值，COUNT >= 3
-- 你的答案：
SELECT
    user_id
FROM (
    SELECT
        user_id,
        login_date,
        DATE_SUB(login_date, INTERVAL ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) DAY) AS base_date
    FROM user_login
) AS same_date
GROUP BY user_id, base_date
HAVING COUNT(*) >= 3



-- ============================================================
-- Q5【环比 - LAG 对比上一期】
-- 按月统计总订单金额，并计算每个月相比「上一个月」的增长金额。
-- 输出：月份, 当月总额, 上月总额, 增长额（第一个月上月为 NULL）
-- ------------------------------------------------------------
-- 考点：DATE_FORMAT 按月分组 + LAG() OVER (ORDER BY 月份)
-- 你的答案：
SELECT
    DATE_FORMAT(order_date, '%Y-%m') AS month,
    SUM(amount) AS current_month_total,
    LAG(SUM(amount)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m')) AS previous_month_total,
    SUM(amount) - LAG(SUM(amount)) OVER (ORDER BY DATE_FORMAT(order_date, '%Y-%m')) AS growth_amount
FROM orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m')



-- ============================================================
-- 写完后：自己每题跑一遍，确认结果合理，再发我批改。
-- 卡住了别急着看提示，先想 5 分钟。
-- ============================================================
