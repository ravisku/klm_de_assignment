-- Top destination country for each season and day of the week based on unique passenger count.

WITH cte_booking_latest AS (
    -- Retrieve the latest booking event for each passenger and flight.
    SELECT
        passenger_uci,
        operating_airline,
        origin_airport,
        destination_airport,
        departure_datetime,
        arrival_datetime,
        booking_status,
        ROW_NUMBER() OVER (
            PARTITION BY passenger_uci, operating_airline, origin_airport, destination_airport, departure_datetime
            ORDER BY event_timestamp DESC
        ) AS latest_event
    FROM bookings
),
confirmed_booking AS (
    -- Filter only CONFIRMED bookings for airline 'KL' originating in the Netherlands.
    SELECT
        passenger_uci,
        origin_airport,
        destination_airport,
        arrival_datetime
    FROM cte_booking_latest
    WHERE 
        booking_status = 'CONFIRMED' 
        AND operating_airline = 'KL' 
        AND latest_event = 1
        AND origin_airport IN (
            SELECT IATA
            FROM airports
            WHERE country = 'Netherlands'
        )
),
booking_country AS (
    -- Map destination airports to their respective countries and calculate arrival dates.
    SELECT
        a.country AS destination_country,
        b.passenger_uci,
        b.destination_airport,
        COALESCE(
            DATE(FROM_UTC_TIMESTAMP(b.arrival_datetime, a.timezone_tz)),
            DATE(b.arrival_datetime)
        ) AS arrival_date
    FROM confirmed_booking b
    LEFT JOIN airports a
        ON b.destination_airport = a.IATA
),
booking_enriched AS (
    -- Add season and day of the week information for each booking.
    SELECT
        destination_country,
        passenger_uci,
        arrival_date,
        DATE_FORMAT(arrival_date, 'EEEE') AS day_of_week,
        CASE 
            WHEN MONTH(arrival_date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(arrival_date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(arrival_date) IN (6, 7, 8) THEN 'Summer'
            WHEN MONTH(arrival_date) IN (9, 10, 11) THEN 'Autumn'
        END AS season
    FROM booking_country
),
unique_passengers AS (
    -- Count unique passengers for each destination, season, and day of the week.
    SELECT
        destination_country,
        season,
        day_of_week,
        COUNT(DISTINCT passenger_uci) AS no_of_passengers
    FROM booking_enriched
    GROUP BY destination_country, season, day_of_week
),
destination_rank AS (
    -- Rank destinations for each season and day of the week based on passenger count.
    SELECT
        destination_country,
        season,
        day_of_week,
        no_of_passengers,
        ROW_NUMBER() OVER (
            PARTITION BY season, day_of_week
            ORDER BY no_of_passengers DESC
        ) AS dest_rank
    FROM unique_passengers
)
-- Retrieve the top-ranked destination for each season and day of the week.
SELECT
    destination_country,
    season,
    day_of_week,
    no_of_passengers
FROM destination_rank
WHERE dest_rank = 1
ORDER BY
    no_of_passengers DESC;
