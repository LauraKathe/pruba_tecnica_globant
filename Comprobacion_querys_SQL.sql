select * FROM hired_employees

    SELECT 
        d.department, 
        j.job,
        SUM(CASE WHEN strftime('%m', e.datetime) BETWEEN '01' AND '03' THEN 1 ELSE 0 END) AS Q1,
        SUM(CASE WHEN strftime('%m', e.datetime) BETWEEN '04' AND '06' THEN 1 ELSE 0 END) AS Q2,
        SUM(CASE WHEN strftime('%m', e.datetime) BETWEEN '07' AND '09' THEN 1 ELSE 0 END) AS Q3,
        SUM(CASE WHEN strftime('%m', e.datetime) BETWEEN '10' AND '12' THEN 1 ELSE 0 END) AS Q4
    FROM hired_employees e
    JOIN departments d ON e.department_id = d.id
    JOIN jobs j ON e.job_id = j.id
    WHERE strftime('%Y', e.datetime) = '2021'
    GROUP BY d.department, j.job
    ORDER BY d.department, j.job;



    WITH dept_hires AS (
        SELECT department_id, COUNT(*) AS hires
        FROM hired_employees
        WHERE strftime('%Y', datetime) = '2021'
        GROUP BY department_id
    ),
    avg_hires AS (
        SELECT AVG(hires) AS avg_hires FROM dept_hires
    )
    SELECT dep.id, dep.department, dh.hires
    FROM departments dep
    JOIN dept_hires dh ON dep.id = dh.department_id,
         avg_hires
    WHERE dh.hires > avg_hires.avg_hires
    ORDER BY dh.hires DESC;