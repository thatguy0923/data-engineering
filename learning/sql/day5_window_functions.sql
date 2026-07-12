-- ============================================
-- Phase 0 Day 7-8: 窗口函数
-- SQL 最能拉开差距的技术，面试必考
-- ============================================

USE employees;

-- ============================================
-- 什么是窗口函数
-- ============================================
-- 普通聚合：GROUP BY 把多行压成一行，丢掉明细
-- 窗口函数：保留每一行，同时在"窗口"内计算聚合值
--
-- 语法：函数名() OVER (PARTITION BY 分组 ORDER BY 排序)
-- ============================================

-- ============================================
-- 1. ROW_NUMBER() — 给每行编号
-- ============================================

-- 需求：给每个部门的员工按薪资排名
SELECT
    d.dept_name,
    e.emp_no,
    e.first_name,
    s.salary,
    ROW_NUMBER() OVER (PARTITION BY de.dept_no ORDER BY s.salary DESC) AS 排名
FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no AND s.to_date = '9999-01-01'
JOIN dept_emp de ON e.emp_no = de.emp_no AND de.to_date = '9999-01-01'
JOIN departments d ON de.dept_no = d.dept_no
ORDER BY d.dept_name, 排名;

-- 🔑 PARTITION BY dept_no → 每个部门内独立编号
--    ORDER BY s.salary DESC → 部门内按薪资从高到低排

-- ============================================
-- 练习 1: 用 ROW_NUMBER() 列出全公司薪资排名前 5 的员工
-- 提示：不用 PARTITION BY，直接 ORDER BY salary DESC，外层 WHERE 排名 <= 5
-- ============================================

-- 你的代码：
SELECT 
    e.emp_no,
    ROW_NUMBER() OVER (ORDER BY salary DESC) AS ranking
FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no AND s.to_date = '9999-01-01'
LIMIT 5;

-- ============================================
-- 2. RANK() vs DENSE_RANK() — 有并列怎么排
-- ============================================

-- RANK()：      1, 2, 2, 4  ← 并列跳号
-- DENSE_RANK()：1, 2, 2, 3  ← 并列不跳号

SELECT
    emp_no,
    salary,
    RANK() OVER (ORDER BY salary DESC) AS rank_rank,
    DENSE_RANK() OVER (ORDER BY salary DESC) AS dense_rank
FROM salaries
WHERE to_date = '9999-01-01'
ORDER BY salary DESC;

-- ============================================
-- 练习 2: 列出每个部门薪资第 1 名的员工
-- 提示：PARTITION BY dept_no，用 RANK() 处理并列，外层 WHERE = 1
-- ============================================

-- 你的代码：
SELECT
    *
FROM (
    SELECT
        s.emp_no,
        de.dept_no,
        RANK() OVER (PARTITION BY de.dept_no ORDER BY s.salary DESC) AS ranking
    FROM salaries s
    JOIN dept_emp de ON s.emp_no = de.emp_no AND de.to_date = '9999-01-01' AND s.to_date = '9999-01-01'
) AS rankings
WHERE ranking = 1;

-- ============================================
-- 3. LAG() / LEAD() — 看前一行、后一行
-- ============================================

-- LAG(列, 偏移量, 默认值) → 往前看
-- LEAD(列, 偏移量, 默认值) → 往后看

-- 需求：看每个员工的薪资变化——上一次薪资是多少
SELECT
    emp_no,
    salary,
    from_date,
    LAG(salary, 1) OVER (PARTITION BY emp_no ORDER BY from_date) AS 上次薪资,
    salary - LAG(salary, 1) OVER (PARTITION BY emp_no ORDER BY from_date) AS 涨幅
FROM salaries
WHERE emp_no IN (10001, 10008)  -- 只看两个人，清楚
ORDER BY emp_no, from_date;

-- 10001: 15000→18000(+3000)→20000(+2000)→24000(+4000)→28000(+4000)
-- 第一行 LAG 是 NULL（没有上一次），所以涨幅也是 NULL

-- ============================================
-- 练习 3: 列出每个员工每次涨薪的涨幅（金额）和涨幅百分比
-- 提示：LAG 取上次薪资，涨幅 = 当前 - 上次，百分比同理
--       用 ROUND 取整
-- ============================================

-- 你的代码：
SELECT
    emp_no,
    salary,
    LAG(salary, 1) OVER (PARTITION BY emp_no ORDER BY from_date) AS '上次薪资',
    salary - LAG(salary, 1) OVER (PARTITION BY emp_no ORDER BY from_date) AS '涨幅金额',
    ROUND((salary - LAG(salary, 1) OVER (PARTITION BY emp_no ORDER BY from_date))
          / LAG(salary, 1) OVER (PARTITION BY emp_no ORDER BY from_date) * 100, 0) AS '涨幅百分比'
FROM salaries;

-- ============================================
-- 4. SUM() OVER — 累计求和（面试最爱）
-- ============================================

-- 需求：看薪资逐月累计（running total）
SELECT
    emp_no,
    salary,
    from_date,
    SUM(salary) OVER (PARTITION BY emp_no ORDER BY from_date) AS 累计薪资
FROM salaries
WHERE emp_no = 10001
ORDER BY from_date;

-- 10001: 15000→33000(15000+18000)→53000→77000→105000

-- 不用窗口函数实现一样的效果：
-- 用关联子查询 SUM(salary) WHERE from_date <= 当前行... 但效率低很多

-- ============================================
-- 5. 移动平均 — ROWS BETWEEN
-- ============================================

-- 窗口框架：在 PARTITION 内再限定"看前几行后几行"
SELECT
    emp_no,
    salary,
    from_date,
    AVG(salary) OVER (
        PARTITION BY emp_no
        ORDER BY from_date
        ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING  -- 当前行+前1+后1，共3行
    ) AS 前后三行平均值
FROM salaries
WHERE emp_no = 10001
ORDER BY from_date;

-- 框架选项：
-- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW   → 从开头累计到当前
-- ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING           → 前1+当前+后1
-- ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING   → 当前到末尾

-- ============================================
-- 练习 4: 列出每个员工当前薪资，以及在本部门的薪资排名（RANK），
--        和本部门比他高一名的薪资（LEAD）
-- 提示：两个窗口函数，PARTITION BY dept_no
--       当前薪资 → WHERE to_date = '9999-01-01'
-- ============================================

-- 你的代码：
SELECT
    e.emp_no,
    s.salary AS '当前薪资',
    RANK() OVER (PARTITION BY de.dept_no ORDER BY s.salary DESC) AS '部门排名',
    LAG(s.salary, 1) OVER (PARTITION BY de.dept_no ORDER BY s.salary DESC) AS '本部门比他高一名的薪资'
FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no AND s.to_date = '9999-01-01'
JOIN dept_emp de ON s.emp_no = de.emp_no AND de.to_date = '9999-01-01';
-- ============================================
-- 窗口函数总结
-- ============================================
-- ROW_NUMBER()    → 强制编号（无并列）
-- RANK()          → 排名（并列跳号）
-- DENSE_RANK()    → 排名（并列不跳号）
-- LAG()           → 看前面
-- LEAD()          → 看后面
-- SUM() OVER      → 累计求和
-- AVG() OVER      → 移动平均
-- PARTITION BY    → 组内计算
-- ROWS BETWEEN    → 限定窗口范围
-- ============================================
