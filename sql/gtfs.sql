COPY (SELECT id as agency_id, jmeno || ', ' || sidlo as agency_name, 'http://' || www as agency_url, 'Europe/Prague' as agency_timezone, 'cs' as agency_lang, telefon_sidlo as agency_phone, email as agency_email
from jdf_dopravci) TO 'C:\Users\admin\PycharmProjects\jdf2\agency.txt' WITH CSV HEADER;
COPY (SELECT id as service_id, po as monday, ut as tuesday, st as wednesday, ct as thursday, pa as friday, so as saturday, ne as sunday, replace(od, '-', '')  as start_date, replace("do", '-', '') as end_date
FROM jdf_kalendar) TO 'C:\Users\admin\PycharmProjects\jdf2\calendar.txt' WITH CSV HEADER;
COPY (SELECT service_id, replace(unnest(string_to_array(datum, ',')), '-', '') as "date", typ as exception_type
FROM jdf_kalendar_vyjimky) TO 'C:\Users\admin\PycharmProjects\jdf2\calendar_dates.txt' WITH CSV HEADER;
COPY (SELECT id as route_id, id_dopravce as agency_id, cislo_linka as route_short_name, nazev_linka as route_long_name, 3 as route_type
FROM jdf_linky WHERE aktualni=TRUE) TO 'C:\Users\admin\PycharmProjects\jdf2\routes.txt' WITH CSV HEADER;
COPY (SELECT trip_id, arrival_time, departure_time, stop_id, ROW_NUMBER () OVER (PARTITION BY trip_id ORDER BY km, departure_time, arrival_time) as stop_sequence FROM
    (SELECT id_spoj as trip_id,
       CASE WHEN prijezd = '' THEN
           CASE WHEN odjezd = '' THEN ''
               ELSE
                    CASE WHEN substring(odjezd from 1 for 1)::integer - substring(FIRST_VALUE(odjezd) OVER (PARTITION BY id_spoj ORDER BY km) from 1 for 1)::integer < 0 THEN (substring(odjezd from 1 for 2)::integer + 24)::char(2) ELSE substring(odjezd from 1 for 2) END
                || ':' || substring(odjezd from 3 for 2) || ':00' END
           ELSE
               CASE WHEN substring(prijezd from 1 for 1)::integer - substring(FIRST_VALUE(odjezd) OVER (PARTITION BY id_spoj ORDER BY km) from 1 for 1)::integer < 0 THEN (substring(prijezd from 1 for 2)::integer + 24)::char(2) ELSE substring(prijezd from 1 for 2) END
            || ':' || substring(prijezd from 3 for 2) || ':00' END as arrival_time,
       CASE WHEN odjezd = '' THEN
           CASE WHEN prijezd = '' THEN ''
               ELSE
                   CASE WHEN substring(prijezd from 1 for 1)::integer - substring(FIRST_VALUE(odjezd) OVER (PARTITION BY id_spoj ORDER BY km) from 1 for 1)::integer < 0 THEN (substring(prijezd from 1 for 2)::integer + 24)::char(2) ELSE substring(prijezd from 1 for 2) END
                || ':' || substring(prijezd from 3 for 2) || ':00' END
           ELSE
               CASE WHEN substring(odjezd from 1 for 1)::integer - substring(FIRST_VALUE(odjezd) OVER (PARTITION BY id_spoj ORDER BY km) from 1 for 1)::integer < 0 THEN (substring(odjezd from 1 for 2)::integer + 24)::char(2) ELSE substring(odjezd from 1 for 2) END
            || ':' || substring(odjezd from 3 for 2) || ':00' END as departure_time,
       id_zastavka as stop_id, km
FROM jdf_odjezdy JOIN jdf_linky ON jdf_odjezdy.id_linka = jdf_linky.id WHERE (odjezd <> '' OR prijezd <> '') AND aktualni = TRUE) t1) TO 'C:\Users\admin\PycharmProjects\jdf2\stop_times.txt' WITH CSV HEADER;
COPY (SELECT id as stop_id, obec || ',' || cast_obce || ',' || misto as stop_name, 0 as stop_lat, 0 as stop_lon
FROM jdf_zastavky) TO 'C:\Users\admin\PycharmProjects\jdf2\stops.txt' WITH CSV HEADER;
COPY (SELECT id_linka as route_id, id_kalendar as service_id, jdf_spoje.id as trip_id
FROM jdf_spoje JOIN jdf_linky ON jdf_spoje.id_linka = jdf_linky.id WHERE aktualni=TRUE) TO 'C:\Users\admin\PycharmProjects\jdf2\trips.txt' WITH CSV HEADER;
