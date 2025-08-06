SELECT COUNT(*) FROM vacancies
SELECT work_location, MAX(salary_to) as max_salary FROM vacancies WHERE work_location IN ('Москва', 'Омск', 'Екатеринбург')   AND date_added >= '2022-10-01'   AND date_added < '2022-11-01' GROUP BY work_location
SELECT vacancies.schedule_type, AVG((vacancies.salary_from  vacancies.salary_to)/2)  FROM vacancies  GROUP BY vacancies.schedule_type
SELECT COUNT(*) FROM vacancies WHERE vacancies.name = 'Java разработчик'
SELECT COUNT(*) FROM vacancies WHERE vacancies.name LIKE '%Python разработчик%' AND vacancies.work_location = 'Санкт-Петербург'
SELECT * FROM vacancies  WHERE name = 'C++ разработчик'  AND (salary_from > 200000 OR salary_to > 200000)
SELECT vacancies.vacancy_id, vacancies.name  FROM vacancies  WHERE vacancies.name = 'Java разработчик'  AND vacancies.salary_from > '150000'  AND vacancies.schedule_type = 'remote'
SELECT COUNT(*) FROM vacancies WHERE vacancies.salary_currency = 'RUR'
SELECT vacancies.work_location, COUNT(*)  FROM vacancies  WHERE vacancies.name LIKE '%Python разработчик%'  GROUP BY vacancies.work_location  ORDER BY COUNT(*) DESC  LIMIT 1
SELECT COUNT(*)  FROM vacancies  WHERE vacancies.salary_from IS NULL AND vacancies.salary_to IS NULL
SELECT salary_currency, MAX(salary_to)  FROM vacancies  GROUP BY salary_currency
SELECT vacancies.vacancy_id, vacancies.name  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE companies.company_city = 'Омск'  AND vacancies.schedule_type = 'flexible'
SELECT DISTINCT(direction) FROM vacancies
SELECT AVG(vacancies.salary_from)  FROM vacancies  WHERE vacancies.work_location = 'Санкт-Петербург'
SELECT MAX(vacancies.salary_to)  FROM vacancies  WHERE vacancies.work_location = 'Москва'
SELECT COUNT(*)  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE companies.name = 'ООО Рога и Копыта'  AND vacancies.salary_from >= 50000  AND vacancies.salary_to <= 100000
SELECT * FROM vacancies WHERE vacancies.date_added = '2020-04-01'
SELECT COUNT(*) FROM vacancies WHERE vacancies.date_added > '2021-01-01'
SELECT * FROM vacancies  WHERE salary_from >= 50000  AND salary_to <= 100000  AND salary_currency = 'RUR'
SELECT DISTINCT(departments.department_code) FROM departments
SELECT COUNT(*) FROM vacancies WHERE vacancies.schedule_type = 'flexible'
SELECT * FROM vacancies WHERE salary_currency = 'USD'
SELECT COUNT(*) FROM vacancies JOIN companies ON vacancies.company_id = companies.company_id WHERE companies.name = 'ООО Ромашка'
SELECT * FROM vacancies WHERE vacancies.date_added = '2021-03-05'
SELECT DISTINCT(vacancies.work_location)  FROM vacancies  WHERE vacancies.schedule_type = 'remote'
SELECT MAX(salary_to) FROM vacancies
SELECT vacancies.*  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE companies.company_street = 'Большая Морская'
SELECT COUNT(*)  FROM vacancies  WHERE level = 'middle'  AND salary_from > 100000  AND salary_currency = 'RUR'
SELECT vacancies.salary_currency, COUNT(*)  FROM vacancies  GROUP BY vacancies.salary_currency  ORDER BY COUNT(*) DESC  LIMIT 1
SELECT COUNT(DISTINCT(companies.company_id))  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE vacancies.schedule_type = 'flexible'
SELECT * FROM vacancies  WHERE work_location = 'Санкт-Петербург'  AND date_added >= '2021-02-01'  AND date_added < '2021-03-01'
SELECT COUNT(*)  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE companies.company_city = 'Омск' AND vacancies.salary_currency = 'USD'
SELECT COUNT(DISTINCT companies.company_id)  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE vacancies.schedule_type = 'fullDay'
SELECT salary_currency, COUNT(*)  FROM vacancies  GROUP BY salary_currency
SELECT COUNT(*) FROM vacancies WHERE vacancies.schedule_type = 'remote'
SELECT companies.name, COUNT(*) as vacancy_count  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE companies.company_city = 'Москва'  GROUP BY companies.name  ORDER BY vacancy_count DESC  LIMIT 5
SELECT work_location, COUNT(*)  FROM vacancies  GROUP BY work_location  ORDER BY COUNT(*) DESC  LIMIT 1
SELECT vacancies.name  FROM vacancies  WHERE vacancies.work_location = 'Москва'  ORDER BY vacancies.salary_to DESC  LIMIT 5
SELECT name, date_added  FROM vacancies  ORDER BY date_added DESC  LIMIT 10
SELECT COUNT(*) FROM vacancies WHERE vacancies.work_location = 'Екатеринбург' AND vacancies.date_added >= '2022-01-01' AND vacancies.date_added < '2022-02-01'
SELECT COUNT(*) FROM vacancies WHERE vacancies.direction = 'it'
SELECT COUNT(*) FROM vacancies WHERE vacancies.level = 'senior' AND vacancies.direction = 'frontend'
SELECT COUNT(*) FROM vacancies WHERE company_id = 5
SELECT COUNT(*) FROM vacancies JOIN companies ON vacancies.company_id = companies.company_id WHERE companies.name = 'Yandex'
SELECT name  FROM vacancies  WHERE salary_from > 5000 AND salary_currency = 'USD'
SELECT COUNT(*) FROM vacancies WHERE vacancies.date_added = CURRENT_DATE
SELECT AVG(vacancies.salary_from)  FROM vacancies  WHERE vacancies.salary_currency = 'USD'
SELECT vacancies.name  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE companies.name = 'ООО Ништяк'
SELECT vacancies.vacancy_id, vacancies.name FROM vacancies WHERE vacancies.date_added > '2022-01-01'
SELECT vacancies.name  FROM vacancies  ORDER BY vacancies.salary_to DESC  LIMIT 10
SELECT departments.department_name  FROM vacancies  JOIN departments ON vacancies.department_id = departments.department_id  WHERE vacancies.vacancy_id = 3
SELECT vacancies.name  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE vacancies.direction = 'backend' AND companies.name LIKE '%Yandex%'
SELECT DISTINCT companies.name  FROM companies  JOIN vacancies ON companies.company_id = vacancies.company_id  WHERE companies.company_city = 'Москва'  AND vacancies.level = 'senior'
SELECT departments.department_id, departments.department_name, departments.department_code  FROM departments  JOIN companies ON departments.company_id = companies.company_id  WHERE companies.name = 'ООО Ништяк'
SELECT COUNT(*)  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE vacancies.schedule_type = 'remote' AND companies.company_city = 'Москва'
SELECT COUNT(*)  FROM vacancies  JOIN departments ON vacancies.department_id = departments.department_id  JOIN companies ON vacancies.company_id = companies.company_id  WHERE departments.department_name = 'finance' AND companies.name = 'ООО Рога и Копыта'
SELECT COUNT(*)  FROM vacancies  WHERE vacancies.level = 'middle'  AND vacancies.date_added >= '2022-01-01'  AND vacancies.date_added < '2023-01-01'  AND vacancies.work_location = 'Санкт-Петербург'
SELECT AVG(v.salary_from)  FROM vacancies v JOIN companies c ON v.company_id = c.company_id WHERE c.company_city = 'Екатеринбург'
SELECT COUNT(*)  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE companies.company_street LIKE '%Ленина%'
SELECT vacancies.name  FROM vacancies  JOIN companies ON vacancies.company_id = companies.company_id  WHERE companies.company_city = 'Санкт-Петербург'  AND vacancies.direction = 'bigdata'
