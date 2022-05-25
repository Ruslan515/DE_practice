set search_path = bookings, public;

-- create table results
create table results
(
    id int,
    response text
);

-- 1. Вывести максимальное количество человек в одном бронировании
INSERT INTO results
SELECT 1,
       count(*) AS max_counts
FROM tickets
GROUP BY book_ref
ORDER BY max_counts DESC
LIMIT 1;

-- 2. Вывести количество бронирований с количеством людей
-- больше среднего значения людей на одно бронирование
insert into results
select
    2,
    count(*)
from (select book_ref, count(ticket_no)
      from tickets
      group by book_ref
      having count(ticket_no) > (select avg(count_tickets)
                                 from (select book_ref, count(ticket_no) as count_tickets
                                       from tickets
                                       group by book_ref
                                       order by count_tickets desc) t1)) t2;

-- 3. Вывести количество бронирований,
-- у которых состав пассажиров повторялся два и более раза,
-- среди бронирований с максимальным количеством людей (п.1)?
insert into results
select
    3,
    count(*)
from (select book_ref, passenger_name, count(*)
      from tickets
      where book_ref in (select book_ref
                         from tickets
                         group by book_ref
                         having count(*) =
                                (select count(*) as max_counts
                                 from tickets
                                 group by book_ref
                                 order by max_counts desc
                                 limit 1))
      group by book_ref, passenger_name
      having count(*) > 1
      order by book_ref, passenger_name) t;

-- 4. Вывести номера брони и контактную информацию по пассажирам
-- в брони (passenger_id, passenger_name, contact_data)
-- с количеством людей в брони = 3
insert into results
select
    4,
    book_ref || '|' || passenger_id || '|' || passenger_name || '|' || contact_data as response
from tickets
where book_ref in (
    select book_ref
    from tickets
    group by book_ref
    having count(*) = 3
    )
order by response;

-- 5. Вывести максимальное количество перелётов на бронь
insert into results
select
    5,
    count(*) as cnt
from tickets t join ticket_flights tf  on t.ticket_no = tf.ticket_no join flights_v f
on tf.flight_id = f.flight_id
group by t.book_ref
order by  cnt desc
limit 1;

-- 6. Вывести максимальное количество перелётов на пассажира в одной брони
insert into results
select
    6,
    count(*) as cnt
from tickets t join ticket_flights tf  on t.ticket_no = tf.ticket_no join flights_v f
on tf.flight_id = f.flight_id
group by t.book_ref, tf.ticket_no
order by  cnt desc
limit 1;

-- 7. Вывести максимальное количество перелётов на пассажира
insert into results
select
    7,
    count(tf.flight_id) as cnt
from tickets t join ticket_flights tf on t.ticket_no = tf.ticket_no
join flights_v f on tf.flight_id = f.flight_id
group by t.passenger_id
order by cnt desc
limit 1;

-- 8. Вывести контактную информацию по пассажиру(ам)
-- (passenger_id, passenger_name, contact_data)
-- и общие траты на билеты, для пассажира потратившему
-- минимальное количество денег на перелеты
insert into results (id, response)
select 8, concat( passenger_id, '|', passenger_name, '|', contact_data, '|', sum(tf.amount) )
from tickets t join ticket_flights tf on (t.ticket_no = tf.ticket_no)
	join flights f on (tf.flight_id = f.flight_id)
where f.status <> 'Cancelled'
group by passenger_id, passenger_name, contact_data 
having sum(tf.amount) = (select sum(tf.amount) as money_spend
						from tickets t join ticket_flights tf on (t.ticket_no = tf.ticket_no)
							join flights f on (tf.flight_id = f.flight_id)
						where f.status <> 'Cancelled'
						group by passenger_id, passenger_name, contact_data 
						order by money_spend 
						limit 1)
order by passenger_id, passenger_name, contact_data, sum(tf.amount) ;

-- ################################### format #####################
insert into results (id, response)
SELECT 8,
		 concat( passenger_id,
		 '|', passenger_name, '|', contact_data, '|', sum(tf.amount) )
FROM tickets t
JOIN ticket_flights tf
	ON (t.ticket_no = tf.ticket_no)
JOIN flights f
	ON (tf.flight_id = f.flight_id)
WHERE f.status <> 'Cancelled'
GROUP BY  passenger_id, passenger_name, contact_data
HAVING sum(tf.amount) = 
	(SELECT sum(tf.amount) AS money_spend
	FROM tickets t
	JOIN ticket_flights tf
		ON (t.ticket_no = tf.ticket_no)
	JOIN flights f
		ON (tf.flight_id = f.flight_id)
	WHERE f.status <> 'Cancelled'
	GROUP BY  passenger_id, passenger_name, contact_data
	ORDER BY  money_spend limit 1)
ORDER BY  passenger_id, passenger_name, contact_data, sum(tf.amount) ; 


-- 9. Вывести контактную информацию по пассажиру(ам)
-- (passenger_id, passenger_name, contact_data) и общее время в полётах,
-- для пассажира, который провёл максимальное время в полётах
insert into results
select
    9,
    t3.passenger_id || '|' || t3.passenger_name || '|' || t3.contact_data
        || '|' || to_char(t3.sum_time_to_fly::varchar(255)::interval, 'hh24:mi:ss') response
from (
    select t2.*, rank() over (order by sum_time_to_fly desc) as rnk
    from (select t1.passenger_id,
                 t1.passenger_name,
                 t1.contact_data,
                 sum(t1.time_to_fly) as sum_time_to_fly
          from (select t.ticket_no,
                       t.passenger_id,
                       t.passenger_name,
                       t.contact_data,
                       tf.flight_id,
                       fv.actual_departure,
                       fv.actual_arrival,
                       EXTRACT(EPOCH FROM AGE(fv.actual_arrival, fv.actual_departure)) as time_to_fly
                from tickets t
                         join ticket_flights tf on t.ticket_no = tf.ticket_no
                         join flights_v fv on tf.flight_id = fv.flight_id
                where fv.status = 'Arrived') t1
          group by t1.passenger_id, t1.passenger_name, t1.contact_data
          order by sum_time_to_fly desc) t2) t3
where t3.rnk = 1
order by response;

-- 10. Вывести город(а) с количеством аэропортов больше одного
insert into results
select
    10,
    city
from airports
group by city
having count(*) > 1
order by city;

-- 11. Вывести город(а), у которого самое меньшее количество городов прямого сообщения
insert into results (id, response)
select 11, arrival_city
from routes
group by arrival_city
having count(distinct departure_city) = (
	select count(distinct departure_city)  as num_routes
	from routes
	group by arrival_city
	order by num_routes 
	limit 1
)
order by arrival_city ;


select 11, arrival_city from bookings_routes group by arrival_city having count(distinct departure_city) = ( select count(distinct departure_city)  as num_routes from bookings_routes group by arrival_city order by num_routes limit 1) order by arrival_city ;

select 11, t2.departure_city as response from (select *, rank() over (order by cnt) as rnk from (select departure_city, count(*) as cnt from bookings_flights_v group by departure_city) t1) t2 where t2.rnk = 1 order by response;

insert into results
with
num_routes as
(
select distinct(departure_city), count(distinct(arrival_city)) as route
from routes
group by departure_city
order by count(distinct(arrival_city))
)
select 11 as id, departure_city as response
from num_routes
where route =   (
            select min(route)
            from num_routes
            )
order by response;

-- 12. Вывести пары городов, у которых нет прямых сообщений исключив реверсные дубликаты
insert into results
select
    12,
    t3.c1 || '|' || t3.c2 as response
from (
    select t2.c1,
           t2.c2
    from (select distinct airports.city as c1,
                        t1.city       as c2
        from airports
                 cross join (select distinct city
                             from airports) t1
        where airports.city < t1.city) t2
    except
    select departure_city as c1,
         arrival_city   as c2
    from flights_v) t3
order by response;

-- 13. Вывести города, до которых нельзя добраться без пересадок из Москвы?
insert into results
select
    13,
    t2.c2 as response
from (select distinct airports.city as c1,
                      t1.city       as c2
      from airports
               cross join (select distinct city
                           from airports) t1
      where airports.city <> t1.city
        and airports.city = 'Москва'
      except
      select departure_city as c1,
             arrival_city   as c2
      from flights_v
      where departure_city = 'Москва') t2
order by response;

-- 14. Вывести модель самолета, который выполнил больше всего рейсов
insert into results
select
    14,
    a.model
from aircrafts a join (
    select *
    from (select *,
                 rank() over (order by cnt desc) as rnk
          from (select aircraft_code,
                       count(*) as cnt
                from flights_v
                where status = 'Arrived'
                group by aircraft_code
                order by count(*) desc) t1) t2
    where rnk = 1) t3 on a.aircraft_code = t3.aircraft_code;

-- 15. Вывести модель самолета, который перевез больше всего пассажиров
insert into results
select
    15,
    a.model
from aircrafts a join (
    select t2.aircraft_code
    from (
        select *,
               rank() over (order by cnt desc) as rnk
        from (select fv.aircraft_code,
                     count(*) as cnt
              from ticket_flights tf
                       join flights_v fv on tf.flight_id = fv.flight_id
              where fv.status = 'Arrived'
              group by fv.aircraft_code) t1) t2
    where rnk = 1) t3 on a.aircraft_code = t3.aircraft_code;

-- 16. Вывести отклонение в минутах
-- суммы запланированного времени перелета от фактического по всем перелётам
insert into results
select
    16,
    abs(sum(plan_time - actual_time))
from (
    select
        EXTRACT(EPOCH FROM AGE(scheduled_arrival, scheduled_departure))/60 as plan_time,
        EXTRACT(EPOCH FROM AGE(actual_arrival, actual_departure))/60 as actual_time
    from flights_v
    where status = 'Arrived') t1;

-- 17. Вывести города, в которые осуществлялся перелёт из Санкт-Петербурга 2016-09-13
insert into results
select
    17,
    arrival_city
from flights_v
where
    departure_city = 'Санкт-Петербург'
  and
    (status = 'Arrived' or status = 'Departed')
  and
    actual_departure::date = '2016-09-13'
group by arrival_city
order by arrival_city;


select
    17,
    arrival_city
from bookings_flights_v
where
    departure_city = 'Санкт-Петербург'
  and
    (status = 'Arrived' or status = 'Departed')
  and
    actual_departure::date = '2016-09-13'
group by arrival_city
order by arrival_city;


-- 18. Вывести перелёт(ы) с максимальной стоимостью всех билетов
insert into results
select
    18,
    t2.flight_id as response
from (
    select *,
           rank() over (order by t1.cnt desc) as rnk
    from (select f.flight_id,
                 sum(tf.amount) as cnt
          from ticket_flights tf
                   join flights f on f.flight_id = tf.flight_id
          where f.status <> 'Cancelled'
          group by f.flight_id
          order by sum(tf.amount) desc) t1) t2
where rnk = 1
order by response;

-- 19. Выбрать дни в которых было осуществлено минимальное количество перелётов
insert into results
select
    19,
    t2.actual_departure as response
from (select *, rank() over (order by cnt) as rnk
      from (select actual_departure::date, count(*) as cnt
            from flights_v
            where status <> 'Cancelled'
              and actual_departure is not null
            group by actual_departure::date
            order by count(*)) t1) t2
where t2.rnk = 1
order by response;

-- 20. Вывести среднее количество вылетов в день из Москвы за 09 месяц 2016 года
insert into results (id, response)
select 20, count(flight_id) / 30
from flights_v fv 
where departure_city = 'Москва' and 
	(status = 'Departed' or status = 'Arrived') and
	date(actual_departure) between '2016-09-01' and '2016-09-30';

-- 21. Вывести топ 5 городов у которых среднее время перелета до пункта назначения больше 3 часов
insert into results (id, response)
select 
	21, departure_city
from flights_v fv 
where status = 'Arrived'
group by departure_city 
having AVG(actual_duration) > '3:00:00'
order by AVG(actual_duration ) desc limit 5;
