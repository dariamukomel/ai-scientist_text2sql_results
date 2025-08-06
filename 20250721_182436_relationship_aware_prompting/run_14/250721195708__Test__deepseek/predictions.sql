SELECT COUNT(*) FROM vacancies
SELECT      work_location,     MAX(salary_to) as max_salary FROM      vacancies WHERE      work_location IN ('Москва', 'Омск', 'Екатеринбург')     AND date_added >= '2022-10-01'      AND date_added < '2022-11-01' GROUP BY      work_location
SELECT      schedule_type,      AVG((salary_from  salary_to) / 2) as avg_salary  FROM      vacancies  GROUP BY      schedule_type
SELECT COUNT(*) FROM vacancies WHERE name = 'Java разработчик'
SELECT COUNT(*) FROM vacancies WHERE name LIKE '%Python разработчик%' AND work_location = 'Санкт-Петербург'
SELECT * FROM vacancies  WHERE name = 'C++ разработчик'  AND (salary_from > 200000 OR salary_to > 200000)
SELECT * FROM vacancies  WHERE name = 'Java разработчик'  AND (salary_from > 150000 OR salary_to > 150000)  AND schedule_type = 'remote'
SELECT COUNT(*) FROM vacancies WHERE salary_currency = 'RUR'
SELECT work_location, COUNT(*)  FROM vacancies  WHERE name LIKE '%Python разработчик%'  GROUP BY work_location  ORDER BY COUNT(*) DESC  LIMIT 1
SELECT COUNT(*)  FROM vacancies  WHERE salary_from IS NULL AND salary_to IS NULL AND salary_currency IS NULL
SELECT salary_currency, MAX(salary_to)  FROM vacancies  GROUP BY salary_currency
SELECT v.name  FROM vacancies v JOIN companies c ON v.company_id = c.company_id WHERE c.company_city = 'Омск' AND v.schedule_type = 'flexible'
SELECT DISTINCT direction FROM vacancies
SELECT AVG(salary_from)  FROM vacancies  WHERE work_location = 'Санкт-Петербург'
SELECT MAX(salary_to) FROM vacancies WHERE work_location = 'Москва'
SELECT COUNT(*)  FROM vacancies v JOIN companies c ON v.company_id = c.company_id WHERE c.name = 'ООО Рога и Копыта'  AND v.salary_from >= 50000  AND v.salary_to <= 100000
SELECT name FROM vacancies WHERE date_added = '2020-04-01'
SELECT COUNT(*) FROM vacancies WHERE date_added > '2021-01-01'
SELECT name, salary_from, salary_to, salary_currency  FROM vacancies  WHERE salary_currency = 'RUR'    AND salary_from >= 50000    AND salary_to <= 100000
SELECT DISTINCT department_name FROM departments
SELECT COUNT(*) FROM vacancies WHERE schedule_type = 'flexible'
SELECT name FROM vacancies WHERE salary_currency = 'USD'
SELECT COUNT(v.vacancy_id)  FROM vacancies v  JOIN companies c ON v.company_id = c.company_id  WHERE c.name = 'ООО Ромашка'
SELECT vacancy_id, name FROM vacancies WHERE date_added = '2021-03-05'
SELECT DISTINCT work_location FROM vacancies WHERE schedule_type = 'remote'
SELECT MAX(salary_to) FROM vacancies
SELECT v.*  FROM vacancies v JOIN companies c ON v.company_id = c.company_id WHERE c.company_street = 'Большая Морская'
SELECT COUNT(*)  FROM vacancies  WHERE level = 'middle'    AND salary_from > 100000    AND salary_currency = 'RUR'
SELECT salary_currency, COUNT(*) as vacancy_count  FROM vacancies  GROUP BY salary_currency  ORDER BY vacancy_count DESC  LIMIT 1
SELECT COUNT(DISTINCT c.company_id)  FROM companies c JOIN vacancies v ON c.company_id = v.company_id WHERE v.schedule_type = 'flexible'
SELECT name  FROM vacancies  WHERE work_location = 'Санкт-Петербург'    AND date_added >= '2021-02-01'    AND date_added < '2021-03-01'
SELECT COUNT(*)  FROM vacancies v JOIN companies c ON v.company_id = c.company_id WHERE c.company_city = 'Омск' AND v.salary_currency = 'USD'
SELECT COUNT(DISTINCT c.company_id)  FROM companies c JOIN vacancies v ON c.company_id = v.company_id WHERE v.schedule_type = 'fullDay'
SELECT salary_currency, COUNT(*)  FROM vacancies  GROUP BY salary_currency
SELECT COUNT(*) FROM vacancies WHERE schedule_type = 'remote'
SELECT c.name, COUNT(v.vacancy_id) as vacancy_count FROM vacancies v JOIN companies c ON v.company_id = c.company_id WHERE c.company_city = 'Москва' GROUP BY c.name ORDER BY vacancy_count DESC LIMIT 5
SELECT work_location, COUNT(*)  FROM vacancies  GROUP BY work_location  ORDER BY COUNT(*) DESC  LIMIT 1
SELECT name  FROM vacancies  WHERE work_location = 'Москва'  ORDER BY salary_to DESC  LIMIT 5
SELECT name, date_added  FROM vacancies  ORDER BY date_added DESC  LIMIT 10
SELECT COUNT(*)  FROM vacancies  WHERE work_location = 'Екатеринбург'    AND date_added >= '2022-01-01'    AND date_added < '2022-02-01'
SELECT COUNT(*) FROM vacancies WHERE direction = 'it'
SELECT COUNT(*) FROM vacancies WHERE level = 'senior' AND direction = 'frontend'
SELECT COUNT(*) FROM vacancies WHERE company_id = 5
SELECT COUNT(v.vacancy_id)  FROM vacancies v  JOIN companies c ON v.company_id = c.company_id  WHERE c.name = 'Yandex'
SELECT name  FROM vacancies  WHERE salary_from > 5000 AND salary_currency = 'USD'
SELECT COUNT(*) FROM vacancies WHERE date_added = CURRENT_DATE
SELECT AVG(salary_from)  FROM vacancies  WHERE salary_currency = 'USD'
SELECT v.name  FROM vacancies v JOIN companies c ON v.company_id = c.company_id WHERE c.name = 'ООО Ништяк'
SELECT vacancy_id, name  FROM vacancies  WHERE date_added > '2022-01-01'
SELECT name, salary_from, salary_to  FROM vacancies  ORDER BY GREATEST(salary_from, salary_to) DESC  LIMIT 10
SELECT d.department_name  FROM vacancies v  JOIN departments d ON v.department_id = d.department_id  WHERE v.vacancy_id = 3
SELECT v.name AS vacancy_name, c.name AS company_name  FROM vacancies v  JOIN companies c ON v.company_id = c.company_id  WHERE v.direction = 'backend' AND c.name LIKE '%Yandex%'
SELECT DISTINCT c.name  FROM companies c JOIN vacancies v ON c.company_id = v.company_id WHERE c.company_city = 'Москва' AND v.level = 'senior'
SELECT d.department_id, d.department_name, d.department_code  FROM departments d  JOIN companies c ON d.company_id = c.company_id  WHERE c.name = 'ООО Ништяк'
SELECT COUNT(*)  FROM vacancies v JOIN companies c ON v.company_id = c.company_id WHERE v.schedule_type = 'remote' AND c.company_city = 'Москва'
SELECT COUNT(v.vacancy_id)  FROM vacancies v JOIN departments d ON v.department_id = d.department_id JOIN companies c ON v.company_id = c.company_id WHERE d.department_name = 'finance' AND c.name = 'ООО Рога и Копыта'
SELECT COUNT(*)  FROM vacancies v JOIN companies c ON v.company_id = c.company_id JOIN cities ci ON c.company_city = ci.city_name WHERE v.level = 'middle'    AND ci.city_name = 'Санкт-Петербург'   AND v.date_added >= '2022-01-01'    AND v.date_added < '2023-01-01'
SELECT AVG(v.salary_from) as avg_salary_from FROM vacancies v JOIN companies c ON v.company_id = c.company_id JOIN cities ci ON c.company_city = ci.city_name WHERE ci.city_name = 'Екатеринбург'
SELECT COUNT(*)  FROM vacancies v JOIN companies c ON v.company_id = c.company_id WHERE c.company_street LIKE '%Ленина%'
SELECT v.name  FROM vacancies v JOIN companies c ON v.company_id = c.company_id WHERE c.company_city = 'Санкт-Петербург' AND v.direction = 'bigdata'
