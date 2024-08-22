drop database if exists nano_employees;
create database nano_employees;
\connect nano_employees;

create table person (id serial primary key, name varchar not null, street varchar, city varchar);

create table company (id serial primary key, name varchar not null, city varchar);

create table works (person_id int not null, company_id int not null, salary decimal(16, 2),
  primary key(person_id, company_id),
  foreign key(person_id) references person(id),
  foreign key(company_id) references company(id));

create table manages (manager_id int not null, employee_id int not null,
  primary key(manager_id, employee_id),
  foreign key(manager_id) references person(id),
  foreign key(employee_id) references person(id));

insert into person (name, street, city) values
  ('Jane', 'Third', 'Los Angeles'),
  ('Karen', 'Sunset', 'Fresno'),
  ('Ellie', 'Hollywood', 'Los Angeles'),
  ('Gwen', 'Qianmen', 'Shengzhou'),
  ('Emily', 'Melrose', 'Los Angeles'),
  ('Joey', 'Beverly', 'Los Angeles'),
  ('Ivan', 'Rodeo', 'Los Angeles'),
  ('Jie', 'La Brea', 'Inglewood'),
  ('Hiro', 'Wilton', 'Beverly Hills'),
  ('Sipho', 'Sixth', 'Los Angeles'),
  ('Luiza', 'Xidan', 'Shengzhou'),
  ('Kameke', 'Park', 'New York'),
  ('Kimiko', 'Yonge', 'Toronto'),
  ('Olivia', 'Xidan', 'Shengzhou');

insert into company (name, city) values
  ('Criteo', 'Paris'),
  ('Fandango', 'Santa Monica'),
  ('Deloitte', 'Los Angeles'),
  ('Bernini', 'Beverly Hills'),
  ('Hulu', 'Los Angeles'),
  ('Google', 'Mountain View'),
  ('Google', 'Venice'),
  ('Librato', 'San Jose'),
  ('Ivivva', 'Santa Monica'),
  ('Sony', 'Culver City'),
  ('Sony', 'Tokyo'),
  ('Criteo', 'São Paulo'),
  ('Criteo', 'Dubai');

insert into works (person_id, company_id, salary) values (1, 5, 125572.27), (1, 8, 130000),
  (2, 1, 93250), (3, 3, 75000), (4, 3, 25000), (5, 1, 450000), (5, 7, 200000),
  (8, 3, 23350), (8, 8, 85750.10), (9, 7, 115000), (10, 9, 132700), (11, 13, 310000),
  (13, 10, 89925), (13, 11, 118000), (14, 13, 400000);

insert into manages values (3, 4), (3, 8), (5, 2), (9, 5), (1, 8), (11, 1), (11, 14);      
