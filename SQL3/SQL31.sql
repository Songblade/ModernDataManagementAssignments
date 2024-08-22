-- employee(ID, person_name, street, city)
-- works(ID, company_name, salary)
-- company(company_name, city)
-- manages(ID, manager_id)

-- Query 1
-- For the employee database (shown below) find the ID of each employee with no manager.
-- Order your results by ascending employee id, return no more than ten tuples.
-- You told us to make no assumptions, so I assumed there might be problems with nulls
select ID from employee
where ID in (select ID from manages where manager_id is null)
	or ID not in (select ID from manages where ID is not null)
order by ID
limit 10;

-- Query 2
-- Return the ID and name of each employee who lives in the same city as the location of the company for which the employee works.
-- Order the results by ascending ID, no more than 5 tuples in the result set.
select employee.ID, employee.person_name
from employee inner join works on employee.ID=works.ID
	inner join company on works.company_name=company.company_name and employee.city=company.city
ORDER BY employee.ID
LIMIT 5;

-- Query 3
-- Return the ID and name of each employee who lives in the same city as does her or his manager”.
-- Order the results by ascending ID, no more than 5 tuples in the result set.
select employee.ID, employee.person_name
from employee inner join manages on employee.ID=manages.ID
	inner join employee as boss on manages.manager_id=boss.ID and employee.city=boss.city
order by employee.ID
limit 5;

-- Query 4
-- Return the ID and name of each employee who earns more than the average salary of all employees in their company.
-- Order the results by ascending ID, no more than 5 tuples in the result set.
select employee.ID, employee.person_name
from employee inner join works on employee.ID = works.ID
where cast(salary as numeric) > 
	(select avg_salary from (select company_name, avg(cast(salary as numeric)) as avg_salary from works group by company_name) as comp_sal
	where works.company_name = comp_sal.company_name)
order by employee.ID
limit 5;

-- If you are wondering, the reason for the casts is that I made salary type money, which unfortunately isn't considered a number

-- Query 5
-- Return the name of company that has the smallest payroll and that company’s payroll.
select company_name, sum(cast(salary as numeric)) as payroll
from works
group by company_name
order by payroll
limit 1;
-- There is probably a better solution that uses the MIN function, but I can't figure out what it is.
