-- ============================================
-- ============================================
-- 场景：HR 要一份完整的员工现状报告
-- 一张表涵盖：基本信息 + 薪资增长 + 部门对比 + 司龄
-- 综合运用：JOIN、CTE、标量子查询、CASE WHEN
-- ============================================

USE employees;
WITH dept_avg AS (
    SELECT de.dept_no, ROUND(AVG(s.salary)) AS avg_sal
    FROM dept_emp de
    JOIN salaries s ON de.emp_no = s.emp_no
    WHERE de.to_date = '9999-01-01'
      AND s.to_date = '9999-01-01'
    GROUP BY de.dept_no
)
SELECT
    CONCAT(e.first_name, e.last_name) AS '姓名',
    d.dept_name AS '部门',
    t.title AS '职位',
    s.salary AS '当前薪资',

    (SELECT salary FROM salaries WHERE emp_no = e.emp_no ORDER BY from_date LIMIT 1) AS '入职薪资',
    ROUND((s.salary - (SELECT salary FROM salaries WHERE emp_no = e.emp_no ORDER BY from_date LIMIT 1))
          / (SELECT salary FROM salaries WHERE emp_no = e.emp_no ORDER BY from_date LIMIT 1) * 100, 0) AS '涨幅百分比',

    da.avg_sal AS '部门平均薪资',

    CASE
        WHEN s.salary > da.avg_sal THEN '高于'
        WHEN s.salary < da.avg_sal THEN '低于'
        ELSE '等于'
    END AS 'vs部门',

    TIMESTAMPDIFF(YEAR, e.hire_date, CURDATE()) AS '司龄',

    (SELECT COUNT(*) FROM salaries s WHERE s.emp_no = e.emp_no) AS '涨薪次数'

FROM employees e
JOIN salaries s ON e.emp_no = s.emp_no AND s.to_date = '9999-01-01'
JOIN dept_emp de ON e.emp_no = de.emp_no AND de.to_date = '9999-01-01'
JOIN departments d ON de.dept_no = d.dept_no
JOIN titles t ON e.emp_no = t.emp_no AND t.to_date = '9999-01-01'
JOIN dept_avg da ON de.dept_no = da.dept_no
ORDER BY 当前薪资 DESC;

-- ============================================
-- 你已经用到的技术点（跑通后回来勾）：
-- □ 4表 JOIN — 关联员工、薪资、部门、职位
-- □ CTE (WITH dept_avg) — 算部门平均薪资
-- □ 标量子查询 ×2 — 入职薪资、涨薪次数
-- □ CASE WHEN — 高于/低于判断
-- □ TIMESTAMPDIFF — 日期计算
-- □ CONCAT — 拼接姓名
-- ============================================
