:: The scripts used so you can easily run them all
:: Query 1
db.employees.find({name: "Kimiko"}).pretty()
:: Query 2
db.employees.find({name: "Luiza"}).pretty()
:: Query 3
db.employees.find({$or: [{city: "Shengzhou"}, {city: "Fresno"}]},
	{name: true, street: true, city: true, _id: false})
.sort({name: 1}).pretty()
:: Query 4
db.employees.find({'companies.name': {$ne: "Deloitte"}})
.sort({name: 1}).limit(4).pretty()
:: Query 5
db.employees.aggregate([
	{$match: {$expr: {$ne: ["$manages", []]}}},
	{$project: {_id: 0, name: 1, "direct reports": {$size: "$manages"}}}
]).pretty()
:: Query 6
db.employees.aggregate([
	{$unwind: "$companies"},
	{$group: {_id: "$companies.city", avg_salary: {$avg: "$companies.salary"}}},
	{$project: {rounded_avg_salary: {$round: ["$avg_salary", 2]}}}
]).pretty()
:: Query 7
db.employees.aggregate([
	{$unwind: "$companies"},
	{$group: {_id: {name: "$name", company: "$companies.name"}}},
	{$group: {_id: "$_id.company", num_people: {$count: {}}}},
	{$sort: {num_people: -1}}, {$limit: 1}
]).pretty()
:: Query 8
db.employees.aggregate([
	{$unwind: "$companies"},
	{$group: {_id: {company_name: "$companies.name", company_city: "$companies.city"}, num_people: {$count: {}}}},
	{$sort: {num_people: -1}}, {$limit: 1}
]).pretty()
:: Query 9
db.employees.aggregate([
	{$unwind: "$companies"},
	{$group: {_id: {name: "$name", company: "$companies.name"}, salary: {$sum: "$companies.salary"}}},
	{$group: {_id: "$_id.company", payroll: {$sum: "$salary"}, num_people: {$count: {}}, min_salary: {$min: "$salary"}, max_salary: {$max: "$salary"}, avg_salary: {$avg: "$salary"}}},
	{$sort: {payroll: -1}}
]).pretty()