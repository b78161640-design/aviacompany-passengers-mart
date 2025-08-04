-- Скрипт создания материализованной витрины passengers_info_mv
-- Цель: агрегировать ключевые показатели по каждому пассажиру
-- Используются таблицы: tickets, ticket_flights, flights, airports, boarding_passes

DROP MATERIALIZED VIEW IF EXISTS passengers_info_mv;

CREATE MATERIALIZED VIEW passengers_info_mv AS

-- 1. Основная агрегированная информация по билетам
WITH ticket_stats AS (
    SELECT
        t.passenger_id,
        t.passenger_name AS fio,
        COUNT(DISTINCT t.ticket_no) AS total_tickets,
        SUM(tf.amount) AS total_tickets_amount,
        (SUM(tf.amount) / COUNT(DISTINCT t.ticket_no))::NUMERIC(10,2) AS avg_tickets_amount
    FROM tickets t
    JOIN ticket_flights tf ON tf.ticket_no = t.ticket_no
    GROUP BY t.passenger_id, t.passenger_name
),

-- 2. Среднее количество перелётов (сегментов) в билете
flight_counts AS (
    SELECT
        t.passenger_id,
        AVG(f.seg_cnt)::NUMERIC(10,2) AS average_flights
    FROM tickets t
    JOIN (
        SELECT ticket_no, COUNT(*) AS seg_cnt
        FROM ticket_flights
        GROUP BY ticket_no
    ) f ON f.ticket_no = t.ticket_no
    GROUP BY t.passenger_id
),

-- 3. Наиболее частые города вылета и прилёта, если один самый частый
most_often_cities AS (
    SELECT
        passenger_id,
        CASE WHEN COUNT(*) FILTER (WHERE rnk_from = 1) = 1 THEN
            MAX(city_from) FILTER (WHERE rnk_from = 1)
        END AS more_often_city_from,
        CASE WHEN COUNT(*) FILTER (WHERE rnk_to = 1) = 1 THEN
            MAX(city_to) FILTER (WHERE rnk_to = 1)
        END AS more_often_city_to
    FROM (
        SELECT
            t.passenger_id,
            a_from.city AS city_from,
            a_to.city AS city_to,
            RANK() OVER (PARTITION BY t.passenger_id ORDER BY COUNT(*) FILTER (WHERE a_from.city IS NOT NULL) DESC) AS rnk_from,
            RANK() OVER (PARTITION BY t.passenger_id ORDER BY COUNT(*) FILTER (WHERE a_to.city IS NOT NULL) DESC) AS rnk_to
        FROM tickets t
        JOIN ticket_flights tf ON tf.ticket_no = t.ticket_no
        JOIN flights f ON f.flight_id = tf.flight_id
        JOIN airports a_from ON a_from.airport_code = f.departure_airport
        JOIN airports a_to ON a_to.airport_code = f.arrival_airport
        GROUP BY t.passenger_id, a_from.city, a_to.city
    ) sub
    GROUP BY passenger_id
),

-- 4. Предпочитаемый аэропорт по названию (если один самый частый)
preferred_airport AS (
    SELECT
        passenger_id,
        CASE WHEN COUNT(*) FILTER (WHERE rnk = 1) = 1 THEN
            MAX(airport_name) FILTER (WHERE rnk = 1)
        END AS preffered_airport
    FROM (
        SELECT
            t.passenger_id,
            ap.airport_name,
            RANK() OVER (PARTITION BY t.passenger_id ORDER BY COUNT(*) DESC) AS rnk
        FROM tickets t
        JOIN ticket_flights tf ON tf.ticket_no = t.ticket_no
        JOIN flights f ON f.flight_id = tf.flight_id
        JOIN airports ap ON ap.airport_code = COALESCE(f.departure_airport, f.arrival_airport)
        GROUP BY t.passenger_id, ap.airport_name
    ) sub
    GROUP BY passenger_id
),

-- 5. Предпочитаемое место (если одно самое частое)
preferred_seat AS (
    SELECT
        passenger_id,
        CASE WHEN COUNT(*) FILTER (WHERE rnk = 1) = 1 THEN
            MAX(seat_no) FILTER (WHERE rnk = 1)
        END AS preffered_seat
    FROM (
        SELECT
            t.passenger_id,
            bp.seat_no,
            RANK() OVER (PARTITION BY t.passenger_id ORDER BY COUNT(*) DESC) AS rnk
        FROM tickets t
        JOIN boarding_passes bp ON bp.ticket_no = t.ticket_no
        GROUP BY t.passenger_id, bp.seat_no
    ) sub
    GROUP BY passenger_id
),

-- 6. Предпочитаемый класс обслуживания
preferred_conditions AS (
    SELECT
        passenger_id,
        CASE WHEN COUNT(*) FILTER (WHERE rnk = 1) = 1 THEN
            MAX(fare_conditions) FILTER (WHERE rnk = 1)
        END AS preffered_conditions
    FROM (
        SELECT
            t.passenger_id,
            tf.fare_conditions,
            RANK() OVER (PARTITION BY t.passenger_id ORDER BY COUNT(*) DESC) AS rnk
        FROM tickets t
        JOIN ticket_flights tf ON tf.ticket_no = t.ticket_no
        GROUP BY t.passenger_id, tf.fare_conditions
    ) sub
    GROUP BY passenger_id
),

-- 7. Контактные данные из JSON
contacts AS (
    SELECT DISTINCT
        passenger_id,
        contact_data ->> 'phone' AS phone_number,
        contact_data ->> 'email' AS email
    FROM tickets
),

-- 8. Суммарное расстояние перелётов (Haversine formula)
flight_distance AS (
    SELECT
        t.passenger_id,
        SUM(
            2 * 6371 * ASIN(
                SQRT(
                    POW(SIN(RADIANS((a_to.latitude - a_from.latitude) / 2)), 2) +
                    COS(RADIANS(a_from.latitude)) * COS(RADIANS(a_to.latitude)) *
                    POW(SIN(RADIANS((a_to.longitude - a_from.longitude) / 2)), 2)
                )
            )
        )::NUMERIC(12,2) AS total_range
    FROM tickets t
    JOIN ticket_flights tf ON tf.ticket_no = t.ticket_no
    JOIN flights f ON f.flight_id = tf.flight_id
    JOIN airports a_from ON a_from.airport_code = f.departure_airport
    JOIN airports a_to ON a_to.airport_code = f.arrival_airport
    GROUP BY t.passenger_id
)

-- Финальный SELECT
SELECT
    ts.passenger_id,
    ts.fio,
    ts.total_tickets,
    ts.total_tickets_amount,
    ts.avg_tickets_amount,
    fc.average_flights,
    mc.more_often_city_from,
    mc.more_often_city_to,
    pa.preffered_airport,
    ps.preffered_seat,
    pc.preffered_conditions,
    c.phone_number,
    c.email,
    fd.total_range
FROM ticket_stats ts
LEFT JOIN flight_counts fc ON fc.passenger_id = ts.passenger_id
LEFT JOIN most_often_cities mc ON mc.passenger_id = ts.passenger_id
LEFT JOIN preferred_airport pa ON pa.passenger_id = ts.passenger_id
LEFT JOIN preferred_seat ps ON ps.passenger_id = ts.passenger_id
LEFT JOIN preferred_conditions pc ON pc.passenger_id = ts.passenger_id
LEFT JOIN contacts c ON c.passenger_id = ts.passenger_id
LEFT JOIN flight_distance fd ON fd.passenger_id = ts.passenger_id;
