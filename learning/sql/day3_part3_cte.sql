-- ============================================
-- Phase 0 Day 3-4: 子查询 + CTE
-- Part 3: CTE（WITH 子句）
-- ============================================
-- CTE = Common Table Expression，给子查询起名
-- 写一次，多次引用，可读性远好于嵌套子查询
-- ============================================

USE employees;

-- ============================================
-- 1. 基本语法
-- ============================================

-- 不用 CTE（子查询嵌了两层）：
SELECT dept_no, dept_name
FROM departments
WHERE dept_no IN (
    SELECT dept_no
    FROM dept_emp
    WHERE to_date = '9999-01-01'
    GROUP BY dept_no
    HAVING COUNT(*) > 2
);

-- 用 CTE 重写：
WITH dept_headcount AS (
    SELECT dept_no, COUNT(*) AS cnt
    FROM dept_emp
    WHERE to_date = '9999-01-01'
    GROUP BY dept_no
)
SELECT d.dept_no, d.dept_name, dh.cnt
FROM departments d
JOIN dept_headcount dh ON d.dept_no = dh.dept_no
WHERE dh.cnt > 2;

-- 🔑 解读：
--   WITH 名字 AS (子查询) — 定义了一个"临时视图"
--   后面可以像查表一样查它
--   整个 WITH ... SELECT 是一次性查询，执行完 CTE 就消失

-- ============================================
-- 2. 多个 CTE — 用逗号串联
-- ============================================

-- 需求：找出当前薪资高于本部门平均薪资的员工，并显示他们跟平均的差额
WITH dept_avg AS (
    -- 每个部门当前平均薪资
    SELECT de.dept_no, ROUND(AVG(s.salary)) AS avg_sal
    FROM dept_emp de
    JOIN salaries s ON de.emp_no = s.emp_no
    WHERE de.to_date = '9999-01-01' AND s.to_date = '9999-01-01'
    GROUP BY de.dept_no
),
emp_current AS (
    -- 每个员工当前薪资 + 部门
    SELECT e.emp_no, e.first_name, e.last_name, s.salary, de.dept_no
    FROM employees e
    JOIN salaries s ON e.emp_no = s.emp_no
    JOIN dept_emp de ON e.emp_no = de.emp_no
    WHERE s.to_date = '9999-01-01' AND de.to_date = '9999-01-01'
)
SELECT ec.emp_no, ec.first_name, ec.last_name, ec.salary,
       da.avg_sal,
       ec.salary - da.avg_sal AS diff
FROM emp_current ec
JOIN dept_avg da ON ec.dept_no = da.dept_no
WHERE ec.salary > da.avg_sal
ORDER BY diff DESC;

-- 🔑 多个 CTE：WITH a AS (...), b AS (...) SELECT ...
--   后面 CTE 可以引用前面定义过的 CTE

-- ============================================
-- 练习 1: 用 CTE 改写 — 找出薪资最高的员工
-- 提示：CTE 算 MAX，外层 JOIN 回来
-- ============================================

-- 你的代码：
WITH max_sal AS (
    SELECT MAX(salary) AS m_sal
    FROM salaries
    WHERE to_date = '9999-01-01'
)
SELECT s.emp_no, ms.m_sal
FROM salaries s 
JOIN max_sal ms ON s.salary = ms.m_sal
WHERE s.to_date = '9999-01-01' ;

-- ============================================
-- 3. CTE 用于复杂统计 — 面试爱考
-- ============================================

-- 需求：每个部门的薪资排名（窗口函数还没学，用 CTE 也能算）
-- 思路：算每个部门的平均薪资 → 给每个员工标排名
WITH dept_avg_sal AS (
    SELECT de.dept_no, de.dept_name, ROUND(AVG(s.salary)) AS d_avg
    FROM departments de
    JOIN dept_emp d ON de.dept_no = d.dept_no
    JOIN salaries s ON d.emp_no = s.emp_no
    WHERE d.to_date = '9999-01-01' AND s.to_date = '9999-01-01'
    GROUP BY de.dept_no, de.dept_name
),
emp_sal AS (
    SELECT e.emp_no, e.first_name, e.last_name, s.salary, de.dept_no
    FROM employees e
    JOIN salaries s ON e.emp_no = s.emp_no
    JOIN dept_emp de ON e.emp_no = de.emp_no
    WHERE s.to_date = '9999-01-01' AND de.to_date = '9999-01-01'
)
SELECT d.dept_name, es.emp_no, es.first_name, es.last_name, es.salary, d.d_avg,
       es.salary - d.d_avg AS above_dept_avg
FROM emp_sal es
JOIN dept_avg_sal d ON es.dept_no = d.dept_no
ORDER BY d.dept_name, es.salary DESC;

-- ============================================
-- 练习 2: 找出每个部门薪资高于该部门平均的员工人数
-- 提示：用两个 CTE — 一个算部门平均，一个拉员工薪资
--       外层 COUNT 满足条件的
-- ============================================

-- 你的代码：
WITH dept_avg_sal AS (
    SELECT de.dept_no, AVG(s.salary) AS average_sal
    FROM salaries s
    JOIN dept_emp de ON s.emp_no = de.emp_no AND de.to_date = '9999-01-01'
    GROUP BY de.dept_no
)
SELECT s.emp_no, de.dept_no, s.salary
FROM salaries s
JOIN dept_emp de ON s.emp_no = de.emp_no AND de.to_date = '9999-01-01'
JOIN dept_avg_sal das ON de.dept_no = das.dept_no
WHERE s.salary > das.average_sal;

-- claude answer:
WITH dept_avg_sal AS (
      SELECT de.dept_no, AVG(s.salary) AS average_sal
      FROM salaries s
      JOIN dept_emp de ON s.emp_no = de.emp_no
      WHERE de.to_date = '9999-01-01' AND s.to_date = '9999-01-01'
      GROUP BY de.dept_no
  ),
  emp_sal AS (
      SELECT s.emp_no, de.dept_no, s.salary
      FROM salaries s
      JOIN dept_emp de ON s.emp_no = de.emp_no
      WHERE de.to_date = '9999-01-01' AND s.to_date = '9999-01-01'
  )
  SELECT emp_sal.dept_no, COUNT(*) AS above_avg_count
  FROM emp_sal
  JOIN dept_avg_sal das ON emp_sal.dept_no = das.dept_no
  WHERE emp_sal.salary > das.average_sal
  GROUP BY emp_sal.dept_no;

-- ============================================
-- 4. 递归 CTE（WITH RECURSIVE）— 了解即可
-- ============================================

-- 用途：处理树形结构（组织架构、菜单层级、目录树）
-- 语法：WITH RECURSIVE ...  UNION ALL  ...

-- 举个简单的生成 1~10 的数字序列：
WITH RECURSIVE numbers AS (
    SELECT 1 AS n                    -- 初始行
    UNION ALL
    SELECT n + 1 FROM numbers        -- 用上一行结果生成下一行
    WHERE n < 10                     -- 终止条件
)
SELECT * FROM numbers;

-- 面试极少考递归 CTE，了解一下就行
-- 工作中用到层级查询（组织架构）时再深挖

-- ============================================
-- 练习 3: 找出部门薪资 TOP 3（每个部门薪资最高的 3 个人）
-- 提示：用子查询做——外层每个人，子查询数"本部门比我高的人"
--       HAVING COUNT(*) < 3 就是 TOP 3
--       （窗口函数 ROW_NUMBER 更简单，但先用子查询理解原理）
-- ============================================

-- 你的代码：
SELECT s.emp_no, s.salary, de.dept_no
FROM dept_emp de
JOIN salaries s ON de.emp_no = s.emp_no AND s.to_date = '9999-01-01'
WHERE (
    SELECT COUNT(*)
    FROM dept_emp de2
    JOIN salaries s2 ON de2.emp_no = s2.emp_no AND s2.to_date = '9999-01-01' AND de2.to_date = '9999-01-01'
    WHERE de2.dept_no = de.dept_no AND s2.salary > s.salary
) < 3 AND de.to_date = '9999-01-01';


-- ============================================
-- Part 3 总结：
-- ✅ 基本 CTE：WITH name AS (SELECT ...) SELECT ... FROM name
-- ✅ 多 CTE：逗号分隔，后面可以引用前面的
-- ✅ CTE vs 子查询：CTE 可读性好，可多次引用；子查询更紧凑
-- ✅ 递归 CTE：了解即可，面试罕见
-- ============================================
