# Витрина пассажиров авиакомпании `passengers_info_mv`

Материализованная витрина, агрегирующая ключевые показатели по пассажирам из базы данных `bookings` (PostgreSQL).  
Витрина разработана в рамках курса по аналитике и используется в отчетности и дашбордах авиакомпании.

## Состав витрины:
- passenger_id
- total_tickets
- total_tickets_amount
- avg_tickets_amount
- average_flights
- more_often_city_from
- more_often_city_to
- preffered_airport
- preffered_seat
- preffered_conditions
- phone_number
- email
- fio
- total_range

## 📄 Скрипт

SQL-скрипт создания витрины: [passengers_info_mv.sql](./passengers_info_mv.sql)

## 📘 Документация

- [📄 Техническое задание](./tech_spec.md)
- [📚 Документация PostgreSQL](./docs/postgresql_doc_link.md)
