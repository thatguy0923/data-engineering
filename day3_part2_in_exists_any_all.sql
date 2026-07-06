-- ============================================
-- Phase 0 Day 3-4: 子查询 + CTE
-- Part 2: IN / EXISTS / ANY / ALL + 关联子查询
-- ============================================
-- 这部分是面试高频考点
-- ============================================

USE employees;

-- ============================================
-- 1. IN — 检查值是否在一个"列表"里
-- ============================================

-- 需求：找出在数据部工作过的所有员工
-- 拆解：先去 dept_emp 查出数据部的 emp_no 列表 → 再用 IN 匹配
SELECT emp_no, first_name, last_name
FROM employees
WHERE emp_no IN (
    SELECT emp_no
    FROM dept_emp
    WHERE dept_no = 'd002'
);

-- ⚠️ IN 和 = 的区别：
-- =  只能比单值 → SELECT ... WHERE x = (子查询) ← 子查询必须返回单行
-- IN 可以比多值 → SELECT ... WHERE x IN (子查询) ← 子查询可以返回多行

-- ============================================
-- 练习 1: 找出至少涨过 2 次薪的员工
-- 提示：在 salaries 里 GROUP BY emp_no，HAVING COUNT(*) >= 2
--       然后外层用 IN
-- ============================================

-- 你的代码：
SELECT emp_no 
FROM salaries 
WHERE emp_no IN (SELECT emp_no FROM salaries GROUP BY emp_no HAVING COUNT(*) >= 2)
GROUP BY emp_no ;
-- ============================================
-- 2. NOT IN — 排除列表里出现过的
-- ============================================

-- 需求：找出从未在工程部待过的员工
SELECT emp_no, first_name, last_name
FROM employees
WHERE emp_no NOT IN (
    SELECT emp_no FROM dept_emp WHERE dept_no = 'd001'
);

-- ============================================
-- ⚠️ NOT IN 的坑（面试常考）
-- ============================================
-- 如果子查询结果里有 NULL，NOT IN 返回空！
-- 因为: x NOT IN (1, 2, NULL) → x != 1 AND x != 2 AND x != NULL → unknown → 永远不成立
-- 安全写法：用 NOT EXISTS 代替（后面讲）

-- ============================================
-- 练习 2: 找出没有薪资记录的员工（造数据时都插了，所以结果是空，但写法要对）
-- ============================================

-- 你的代码：
SELECT emp_no
FROM employees
WHERE emp_no NOT IN (SELECT emp_no FROM salaries);
-- ============================================
-- 3. 关联子查询（Correlated Subquery）— 子查询依赖外层每行
-- ============================================

-- 非关联：子查询独立跑一次，外层用结果
-- 关联：  外层每行都触发一次子查询，子查询引用外层列

-- 需求：找出当前薪资高于本部门平均薪资的员工
-- 思路：外层一行行看每个员工 → 子查询用这行的 dept_no 算出该部门平均
SELECT e.emp_no, e.first_name, e.last_name, s.salary, d.dept_name
FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no AND s.to_date = '9999-01-01'
JOIN dept_emp de ON e.emp_no = de.emp_no AND de.to_date = '9999-01-01'
JOIN departments d ON de.dept_no = d.dept_no
WHERE s.salary > (
    -- 关联子查询：注意这里的 dept_no 来自外层 de.dept_no
    SELECT AVG(s2.salary)
    FROM dept_emp de2
    JOIN salaries s2 ON de2.emp_no = s2.emp_no
    WHERE de2.dept_no = de.dept_no           -- ← 这行引用了外层！
      AND s2.to_date = '9999-01-01'
      AND de2.to_date = '9999-01-01'
)
ORDER BY d.dept_name, s.salary DESC;

-- 🔑 理解关联子查询：想想外层每行数据去问内层"我这个部门的平均薪资是多少"
--   外层 10001（在数据部）→ 子查询算数据部平均 19000 → 10001 薪资 28000 > 19000 ✅
--   外层 10007（在数据部）→ 子查询算数据部平均 19000 → 10007 薪资 15500 > 19000 ❌

-- ============================================
-- 练习 3: 找出入职时间早于本部门平均入职时间的员工
-- 提示：关联子查询用 AVG(hire_date) 算部门平均入职时间
-- ============================================

-- 你的代码：
SELECT e.emp_no
FROM employees e
JOIN dept_emp de ON e.emp_no = de.emp_no AND de.to_date = '9999-01-01'
WHERE e.hire_date < (
    SELECT AVG(e2.hire_date)
    FROM employees e2
    JOIN dept_emp de2 ON e2.emp_no = de2.emp_no AND de2.to_date = '9999-01-01'
    WHERE de2.dept_no = de.dept_no
)
-- ============================================
-- 4. EXISTS — 检查"是否存在"
-- ============================================

-- 需求：找出当前持有多个 title 的员工（虽然少见，但写法重要）
-- 先用简单例子：找出有薪资记录的所有员工
SELECT emp_no, first_name, last_name
FROM employees e
WHERE EXISTS (
    SELECT 1 FROM salaries s WHERE s.emp_no = e.emp_no
);

-- EXISTS 是布尔值：子查询有结果 → TRUE，没结果 → FALSE
-- SELECT 1 是惯用写法，因为 EXISTS 不关心具体值
-- 性能通常比 IN 好（找到一条就停，不等扫完）

-- ============================================
-- EXISTS vs IN 对比（面试考点）
-- ============================================
-- IN:    先跑子查询拿到列表 → 外层逐行比对
-- EXISTS: 外层每行都跑一次子查询 → 找到就停
-- 子查询结果集大时 EXISTS 更快
-- NOT EXISTS 不会被 NULL 坑，NOT IN 会

-- ============================================
-- 练习 4: 找出没有任何薪资记录超过 30000 的员工
-- 提示：NOT EXISTS，关联子查询查 salaries 里 > 30000 的
-- ============================================

-- 你的代码：
SELECT emp_no, first_name, last_name
FROM employees e
WHERE NOT EXISTS (
    SELECT 1 FROM salaries s
    WHERE s.emp_no = e.emp_no
      AND s.salary > 30000
);
-- ============================================
-- 5. ANY / ALL — 跟一组值逐一比较
-- ============================================

-- ANY: 比任意一个大/小就行
-- ALL: 必须比所有都大/小

-- 需求：找出薪资比所有初级岗（Junior% 用 LIKE）都高的员工
SELECT emp_no, salary
FROM salaries
WHERE to_date = '9999-01-01'
  AND salary > ALL (
      SELECT s.salary
      FROM salaries s
      JOIN titles t ON s.emp_no = t.emp_no
      WHERE t.title LIKE 'Junior%'
        AND s.to_date = '9999-01-01'
        AND t.to_date = '9999-01-01'
  );

-- ============================================
-- 练习 5: 找出当前薪资比任一高级岗位（Senior%）还低的员工
-- 提示：salary < ANY (...)
-- ============================================

-- 你的代码：
SELECT emp_no, salary
FROM salaries
WHERE to_date = '9999-01-01'
    AND salary < ANY (
        SELECT s.salary
        FROM salaries s
        JOIN titles t ON s.emp_no = t.emp_no
        WHERE t.title LIKE 'Senior%'
          AND s.to_date = '9999-01-01'
          AND t.to_date = '9999-01-01'
    );
-- ============================================
-- Bonus: > ALL 等价于 > (SELECT MAX(...))
--        > ANY 等价于 > (SELECT MIN(...))
-- 但 MIN/MAX 只能用标量子查询，占位不同
-- ============================================
