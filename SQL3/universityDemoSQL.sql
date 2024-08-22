-- Query 6
-- “Return all instructors, but include only their IDs and the number of sections taught by that instructor.”
-- If an instructor has not taught any section, return 0.
-- Order the result set by ascending instructor id, return no more than five results.

select instructor.ID, count(teaches.sec_id)
from instructor left join teaches on instructor.ID = teaches.ID
group by instructor.ID
order by instructor.ID
limit 5;

-- Query 7
-- “Return all course sections offered in 2008, together with the ID and name of each instructor teaching the section”.
-- If a section has more than one instructor, that section should appear as many times in the result as it has instructors.
-- If a section does not have any instructor, it should still appear in the result with the instructor name set to “—”.
-- Each tuple must include: year, semester, course id, section id, instructor id, instructor name
-- Order by ascending year, and, within that ordering, by ascending course id.
-- Return no more than twelve tuples.

select section.year, section.semester, section.course_id, section.sec_id, 
	coalesce(teaches.ID, '-') as ID, coalesce(instructor.name, '—') as name
from section full join teaches on section.course_id=teaches.course_id and section.sec_id=teaches.sec_id
		and section.semester=teaches.semester and section.year=teaches.year
	left join instructor on teaches.ID=instructor.ID
where section.year=2008
order by section.course_id
limit 12;

-- Query 8
-- “Return all department names together with the total number of instructors in each department.”
-- The result set must be ordered by ascending number of instructors, and within that ordering, by ascending department name.
-- Return no more than seven tuples.
-- Departments that have no instructors and departments that have zero instructors must still be included in the result set.
select dept_name, (select count(ID) from instructor where department.dept_name=instructor.dept_name) as num_instructors from department
order by num_instructors, dept_name
limit 7;

-- One second, this is also an obvious group-by problem. I'm realizing that nested subqueries are best not done if possible, since they add confusion
select department.dept_name, count(instructor.ID) as num_instructors
from department left join instructor on department.dept_name = instructor.dept_name
group by department.dept_name
order by num_instructors, dept_name
limit 7;

