-- ============================================
-- Phase 0 Day 11-12: 索引 + EXPLAIN + 查询优化
-- 面试能说出这几句话，SQL 优化就过关了
-- ============================================

USE employees;

-- ============================================
-- 1. EXPLAIN — 看 SQL 怎么执行的
-- ============================================

-- 在查询前面加 EXPLAIN，看执行计划
EXPLAIN
SELECT e.emp_no, e.first_name, s.salary
FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no
WHERE s.salary > 30000
  AND s.to_date = '9999-01-01';

-- 关注这几列：
-- type：  ALL（全表扫）→ INDEX（索引扫）→ range（范围）→ ref（引用）→ const（常量）
--        从差到好，ALL 最差
-- rows： 扫描行数，越小越好
-- key：  实际用了哪个索引，NULL = 没用
-- Extra：Using filesort（额外排序，不好）/ Using temporary（临时表，不好）/ Using index（好）

-- ============================================
-- 练习 1: EXPLAIN 对比 — 有/无 WHERE 条件
-- EXPLAIN SELECT * FROM salaries;
-- EXPLAIN SELECT * FROM salaries WHERE emp_no = 10001;
-- 对比 type 和 rows
-- ============================================

-- 你的代码：
EXPLAIN SELECT * FROM salaries;
EXPLAIN SELECT * FROM salaries WHERE emp_no = 10001;

-- ============================================
-- 2. CREATE INDEX — 建索引
-- ============================================

-- 查看现有索引
SHOW INDEX FROM salaries;

-- 建索引：对常出现在 WHERE/JOIN/ORDER BY 的列
CREATE INDEX idx_salaries_sal ON salaries(salary);

-- 再跑 EXPLAIN，看看 key 列变了没
EXPLAIN
SELECT emp_no, salary
FROM salaries
WHERE salary > 40000
  AND to_date = '9999-01-01';

-- 删索引（索引太多会拖慢写入）
DROP INDEX idx_salaries_sal ON salaries;

-- 🔑 索引不是越多越好 — 每 INSERT/UPDATE 一次，索引也要更新
--    一般给 WHERE / JOIN 列加，别的算了

-- ============================================
-- 练习 2: 给 dept_emp 加索引，看 JOIN 变快
-- ① EXPLAIN 看 dept_emp JOIN employees（没额外索引时的执行计划）
-- ② 加索引 → 再看
-- ③ 删索引恢复原状
-- ============================================

-- 你的代码：
CREATE INDEX idx_dept_emp_emp_no ON dept_emp(emp_no);
EXPLAIN SELECT *
FROM dept_emp de
JOIN employees e ON de.emp_no = e.emp_no;
 DROP INDEX idx_dept_emp_emp_no ON dept_emp;
-- ============================================
-- 3. 什么时候索引没用
-- ============================================

-- ❌ LIKE '%xxx' — 前模糊，索引失效
-- ❌ WHERE salary * 1.1 > 50000 — 列上做运算，索引失效
-- ❌ WHERE ... OR ... — OR 两边不同列，可能不走索引
-- ❌ 小表 — MySQL 觉得全表扫更快

-- 演示：列上做运算
EXPLAIN SELECT * FROM salaries WHERE salary + 0 > 40000;  -- 索引可能失效

-- 演示：OR
EXPLAIN SELECT * FROM employees WHERE emp_no = 10001 OR first_name = '张';  -- 跨列 OR

-- ============================================
-- 4. 复合索引 — 面试加分
-- ============================================

-- 给多列一起建索引
CREATE INDEX idx_sal_emp_date ON salaries(emp_no, to_date);

-- 左前缀原则：索引 (A, B, C) 对 A / A+B / A+B+C 有效
--                对 B 单独 / B+C / C 单独 → 无效
-- 就好比电话簿先按姓排再按名排 → 你知道姓查得快，只知道名就得逐页翻

EXPLAIN
SELECT salary FROM salaries
WHERE emp_no = 10001 AND to_date = '9999-01-01';  -- ✅ 用到索引

EXPLAIN
SELECT salary FROM salaries
WHERE to_date = '9999-01-01';  -- ❌ 只查第二列，索引失效

DROP INDEX idx_sal_emp_date ON salaries;

-- ============================================
-- 练习 3: 面试模拟 — 给出优化建议
-- ============================================
-- 场景：HR 说下面这个查询很慢，你拿到后怎么优化？
SELECT e.emp_no, e.first_name, d.dept_name, s.salary
FROM employees e
JOIN dept_emp de ON e.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
JOIN salaries s ON e.emp_no = s.emp_no
WHERE e.first_name LIKE '%张%'
  AND s.salary > 20000
  AND de.to_date = '9999-01-01';

-- 你的优化思路（写注释即可，不用真的改代码）：

-- 给where里的s.salary和de.to_date创建索引

-- ============================================
-- 5. 常见优化速查
-- ============================================

-- | 问题                         | 方案                            |
-- |------------------------------|----------------------------------|
-- | SELECT *                     | 只查需要的列                     |
-- | 大表 JOIN 小表               | 小表放前面（MySQL 8 自动优化）    |
-- | WHERE 列上套函数              | 把函数移到比较值上                |
-- | LIKE '%xxx'                  | 考虑全文索引或者换 ES             |
-- | 子查询太深                    | 改写成 JOIN 或 CTE               |
-- | 数据量大                     | 加 LIMIT，分页查                  |
-- | 慢查询不知道怎么来的           | EXPLAIN 看一下 type=ALL 的       |

-- ============================================
-- 总结：面试 3 句话
-- ============================================
-- 1. "慢查询先用 EXPLAIN 看执行计划，关注 type 和 rows"
-- 2. "给 WHERE/JOIN 列加索引，注意复合索引的左前缀原则"
-- 3. "避免在 WHERE 列上做运算、避免前模糊 LIKE，必要时改写成 JOIN"
-- ============================================
