-- ============================================
-- Phase 0 Day 3-4: 子查询 + CTE
-- Part 1: 标量子查询（Scalar Subquery）
-- ============================================
-- 标量子查询 = 返回"单个值"的子查询
-- 可以用在 SELECT、WHERE、HAVING 中
-- 类比：把它当成一个临时的"变量"
-- ============================================

USE employees;

-- ============================================
-- 1. 子查询用在 WHERE 中
-- ============================================

-- 需求：找出薪资高于公司平均薪资的员工
-- 思路：先算平均薪资（子查询），再筛人（外层）
SELECT emp_no, salary, from_date
FROM salaries
WHERE to_date = '9999-01-01'              -- 只看当前薪资
  AND salary > (SELECT AVG(salary)          -- 子查询：全公司当前平均
                FROM salaries
                WHERE to_date = '9999-01-01')
ORDER BY salary DESC;

-- ✅ 运行后回答：有几个人高于平均？

-- ============================================
-- 练习 1：找出工资最高的员工是谁
-- 提示：子查询用 MAX，外层用 =
-- ============================================

-- 你的代码：
SELECT emp_no FROM salaries WHERE to_date = '9999-01-01' AND salary = (SELECT MAX(salary) FROM salaries WHERE to_date = '9999-01-01');

-- ============================================
-- 2. 子查询用在 SELECT 中
-- ============================================

-- 需求：列出所有员工当前薪资，同时显示与公司平均的差额
SELECT
    e.emp_no,
    e.first_name,
    e.last_name,
    s.salary,
    (SELECT AVG(salary) FROM salaries WHERE to_date = '9999-01-01') AS avg_salary,
    s.salary - (SELECT AVG(salary) FROM salaries WHERE to_date = '9999-01-01') AS diff_from_avg
FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no AND s.to_date = '9999-01-01'
ORDER BY diff_from_avg DESC;

-- 正数 = 高于平均，负数 = 低于平均

-- ============================================
-- 练习 2：列出员工 + 薪资 + 显示"公司最高薪"和"差距"
-- ============================================

-- 你的代码：
SELECT emp_no, salary,
    (SELECT MAX(salary) FROM salaries WHERE to_date = '9999-01-01') AS '最高薪资',
    (SELECT MAX(salary) FROM salaries WHERE to_date = '9999-01-01') - salary AS '差距'
FROM salaries WHERE to_date = '9999-01-01';

-- ============================================
-- 3. 子查询用在 HAVING 中
-- ============================================

-- 需求：找出平均薪资高于全公司整体平均薪资的部门
SELECT
    d.dept_no,
    d.dept_name,
    ROUND(AVG(s.salary)) AS dept_avg
FROM departments d
JOIN dept_emp de ON d.dept_no = de.dept_no AND de.to_date = '9999-01-01'
JOIN salaries s ON de.emp_no = s.emp_no AND s.to_date = '9999-01-01'
GROUP BY d.dept_no, d.dept_name
HAVING AVG(s.salary) > (
    SELECT AVG(salary) FROM salaries WHERE to_date = '9999-01-01'
)
ORDER BY dept_avg DESC;

-- 这时你发现数据部的人很多，但平均不一定最高——因为有初级岗拉低

-- ============================================
-- 练习 3：找出人数多于全公司部门平均人数的部门
-- 提示：子查询算 avg(部门人数)
-- ============================================

-- 你的代码：
SELECT de.dept_no, d.dept_name
FROM salaries s
JOIN dept_emp de ON s.emp_no = de.emp_no
JOIN departments d ON de.dept_no = d.dept_no
WHERE s.to_date = '9999-01-01'
  AND de.to_date = '9999-01-01'
GROUP BY de.dept_no
HAVING COUNT(s.emp_no) > (
    SELECT AVG(cnt) FROM (
        SELECT COUNT(*) AS cnt
        FROM dept_emp
        WHERE to_date = '9999-01-01'
        GROUP BY dept_no
    ) AS t
);

-- ============================================
-- 4. ⚠️ 标量子查询的坑
-- ============================================

-- 坑1: 如果子查询返回多行 → 报错
-- 下面这句会报错：Subquery returns more than 1 row
-- SELECT * FROM employees
-- WHERE emp_no = (SELECT emp_no FROM salaries WHERE salary > 20000);

-- 坑2: 返回 NULL → 外层也变 NULL（但不会报错）
-- 坑3: 关联 vs 非关联 — 非关联只执行一次，关联随每行执行

-- 先确认数据长什么样：
-- SELECT * FROM employees LIMIT 5;
-- SELECT * FROM salaries LIMIT 5;
-- SELECT * FROM dept_emp LIMIT 5;
-- SELECT * FROM titles LIMIT 5;
-- SELECT * FROM departments;
