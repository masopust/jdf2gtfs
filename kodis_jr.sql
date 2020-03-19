select jdf_linky.cislo_linka::integer, jdf_odjezdy.cislo_spoj,
jdf_odjezdy.cislo_tarifni, jdf_odjezdy.km,
jdf_zastavky.cislo_zastavka,
(jdf_zastavky.obec || ',' || jdf_zastavky.cast_obce || ',' || jdf_zastavky.misto) as zastavka,
jdf_linky.nazev_linka, jdf_dopravci.jmeno as dopravce,
to_timestamp((case when jdf_odjezdy.odjezd = '' then jdf_odjezdy.prijezd else jdf_odjezdy.odjezd end),'HH24MI')::time as cas,
to_date(jdf_linky.platnost_od,'DDMMYYYY') as platnost_od,
to_date(jdf_linky.platnost_do,'DDMMYYYY') as platnost_do,
coalesce(
case when (((lag(jdf_linky.cislo_linka) over (order by to_date(jdf_linky.platnost_od,'DDMMYYYY'),jdf_linky.cislo_linka,jdf_odjezdy.cislo_spoj,km,to_timestamp((case when jdf_odjezdy.odjezd = '' then jdf_odjezdy.prijezd else jdf_odjezdy.odjezd end),'HH24MI')::time))=jdf_linky.cislo_linka) and
		   ((lag(jdf_odjezdy.cislo_spoj) over (order by to_date(jdf_linky.platnost_od,'DDMMYYYY'),jdf_linky.cislo_linka,jdf_odjezdy.cislo_spoj,km,to_timestamp((case when jdf_odjezdy.odjezd = '' then jdf_odjezdy.prijezd else jdf_odjezdy.odjezd end),'HH24MI')::time))=jdf_odjezdy.cislo_spoj)) then null else 'počáteční zastávka' end,
case when (((lead(jdf_linky.cislo_linka) over (order by to_date(jdf_linky.platnost_od,'DDMMYYYY'),jdf_linky.cislo_linka,jdf_odjezdy.cislo_spoj,km,to_timestamp((case when jdf_odjezdy.odjezd = '' then jdf_odjezdy.prijezd else jdf_odjezdy.odjezd end),'HH24MI')::time))=jdf_linky.cislo_linka) and
		   ((lead(jdf_odjezdy.cislo_spoj) over (order by to_date(jdf_linky.platnost_od,'DDMMYYYY'),jdf_linky.cislo_linka,jdf_odjezdy.cislo_spoj,km,to_timestamp((case when jdf_odjezdy.odjezd = '' then jdf_odjezdy.prijezd else jdf_odjezdy.odjezd end),'HH24MI')::time))=jdf_odjezdy.cislo_spoj)) then null else 'konečná zastávka' end) as typ_zastavky,
       jdf_kalendar.po, jdf_kalendar.ut, jdf_kalendar.st, jdf_kalendar.ct, jdf_kalendar.pa, jdf_kalendar.so, jdf_kalendar.ne, jdf_kalendar.prac_den, jdf_kalendar.svatek, jdf_kalendar.od, jdf_kalendar.do, jede.datum as jede_take, nejede.datum as nejede
into msk2018_jr
from jdf_odjezdy
join jdf_linky on jdf_odjezdy.id_linka=jdf_linky.id
join jdf_spoje on jdf_odjezdy.id_spoj=jdf_spoje.id
join jdf_zastavky on jdf_odjezdy.id_zastavka = jdf_zastavky.id
join jdf_dopravci on jdf_linky.id_dopravce = jdf_dopravci.id
left join jdf_kalendar on jdf_spoje.id_kalendar = jdf_kalendar.id
left join jdf_kalendar_vyjimky jede on ((jdf_spoje.id_kalendar = jede.service_id) and (jede.typ = 1))
left join jdf_kalendar_vyjimky nejede on ((jdf_spoje.id_kalendar = nejede.service_id) and (nejede.typ = 2))
where platnost_od <> platnost_do
order by jdf_linky.cislo_linka,jdf_odjezdy.cislo_spoj,km,cas