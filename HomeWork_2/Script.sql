--first SQL

select c.name , count(film_id) as count_films
from film_category fc
join category c on c.category_id  =fc.category_id
group by c.name
order by 2 desc;

--Second SQL

select  a.actor_id , a.last_name || ' ' || a.first_name as Actor_Name , count(r.rental_id)as conut_rent from actor a
join film_actor fa on fa.actor_id  = a.actor_id
join inventory i on fa.film_id = i.film_id
join rental r on r.inventory_id  =i.inventory_id
group by a.actor_id , a.last_name || ' ' || a.first_name
order by 3 desc
limit 10;


--Third SQL

select c.name , sum(p.amount) as sum_payment from category c
join film_category fc  on c.category_id  = fc.category_id
join inventory i on fc.film_id = i.film_id
join rental r on r.inventory_id = i.inventory_id
join payment p on p.rental_id  = r.rental_id
group by c."name"
order by 2 desc
limit 1;

--fourth
select f.title from film f
left join inventory i on f.film_id  = i.film_id
where i.inventory_id  is null;


--fifth
select actor_id , actor_name, count_frequency from (
select a.actor_id , a.last_name || ' ' || a.first_name as Actor_Name, count(c.category_id) as count_frequency, rank() over( order by count(c.category_id) desc ) as rn
from actor a
join film_actor fa on fa.actor_id  = a.actor_id
join film_category fc on fc.film_id = fa.film_id
join category c on c.category_id = fc.category_id and c."name" ='Children'
group by a.actor_id , a.last_name || ' ' || a.first_name) as d
where rn <=3;

--sixth
select * from (
select c2.city
, sum(case when c.active = 1 then 1 else 0 end) as active_clients
, sum(case when c.active != 1 then 1 else 0 end) as not_active
from customer c
join address a on c.address_id  = a.address_id
join city c2 on c2.city_id  = a.city_id
group by c2.city )d
order by not_active desc;


--seventh
select name, sum_rental_duration, segment from
	(select c3."name" , sum(f.rental_duration) as sum_rental_duration, row_number () over(order by sum(f.rental_duration))as rn,  'City begin A' as segment
	 from film_category fc
		join category c3 on fc.category_id  = c3.category_id
		join film f on f.film_id = fc.film_id
		join inventory i on i.film_id = fc.film_id
		join rental r on r.inventory_id = i.inventory_id
		join customer c2 on r.customer_id = c2.customer_id
		join address a on c2.address_id  = a.address_id
		join city c on c.city_id  = a.city_id  and lower(c.city ) like 'a%'
	group by c3."name"
	union
	select c3."name" ,sum(f.rental_duration) as sum_rental_duration, row_number () over(order by sum(f.rental_duration))as  rn, 'City has symbol "-" ' as segment
	from film_category fc
		join category c3 on fc.category_id  = c3.category_id
		join film f on f.film_id = fc.film_id
		join inventory i on i.film_id = fc.film_id
		join rental r on r.inventory_id = i.inventory_id
		join customer c2 on r.customer_id = c2.customer_id
		join address a on c2.address_id  = a.address_id
		join city c on c.city_id  = a.city_id  and lower(c.city ) like '%-%'
	group by c3."name" ) d
where rn = 1;
