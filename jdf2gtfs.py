#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import zipfile
import shutil
import urllib.request as request
from contextlib import closing
import csv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Sequence, Column, Integer, BOOLEAN, VARCHAR, CHAR, TEXT
from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
import pandas as pd
from pandas.tseries.holiday import *
from datetime import datetime
import argparse

def find_sensitive_path(dir, insensitive_file_name):

    for name in os.listdir(dir):
        if insensitive_file_name.lower() == name.lower():
            return name
    return None

parser = argparse.ArgumentParser(
    description='...')
parser.add_argument("in_dir", help="Path to input direcotry with JDF files or zip. Eventually directory where files from CIS JR will be unzipped and stored")
parser.add_argument("out_dir", help="Path to output direcotry with GTFS files")
parser.add_argument("--db_name", action="store", help="PostgreSQL database name",default= "postgres")
parser.add_argument("--db_server",action="store", help="PostgreSQL database server",default= "postgres")
parser.add_argument("--db_user",action="store", help="PostgreSQL database username",default= "postgres")
parser.add_argument("--db_password", action="store", help="PostgreSQL database password",default= "postgres")
parser.add_argument("--store_db_tables", action="store_true", help="If database tables in PostgreSQL database will be stored or deleted")


parser.add_argument("--cisjr",action="store_true", help="Download data from CISJR")
parser.add_argument("--zip",action="store_true", help="Input JDF data are in zip files")
parser.add_argument("--keep_history",action="store_true", help="If processing more timetables from different times")

#parser.add_argument("--dirzip",action="store_true", help="Path to output json file with location history")
#parser.add_argument("--subdir",action="store_true", help="Path to output json file with location history")

parser.add_argument("--stopids", action="store_true", help="Use stop IDs as a primary key")
parser.add_argument("--stopnames", action="store_true", help="Use stop names as a primary key")
parser.add_argument("--stoplocations", action="store", help="Path to csv file with stops locations. (ID,x,y) or (name,x,y)")

args = parser.parse_args()

DATABASE_URL = f"postgresql://{args.db_user}:{args.db_password}@{args.db_server}/{args.db_name}"
engine = create_engine(DATABASE_URL, echo=True)  # show debug information
Session = sessionmaker(bind=engine)
dbsession = Session()
dbsession.autoflush = False

Base = declarative_base()


class JdfZastavka(Base):
    __tablename__ = "jdf_zastavky"  # name of the table
    id = Column(Integer, Sequence("jdf_zastavky_id"), primary_key=True)
    cislo_zastavka = Column(Integer)  # je treba?
    obec = Column(VARCHAR(48))
    cast_obce = Column(VARCHAR(48))
    misto = Column(VARCHAR(48))
    blizka_obec = Column(VARCHAR(3))
    stat = Column(VARCHAR(3))

class JdfZastavkaPoloha(Base):
    __tablename__ = "jdf_zastavky_polohy"  # name of the table
    id = Column(Integer, Sequence("jdf_zastavky_polohy_id"), primary_key=True)
    cislo_zastavka = Column(Integer)  # je treba?
    nazev = Column(VARCHAR(150))
    x = Column(DOUBLE_PRECISION)
    y = Column(DOUBLE_PRECISION)

class JdfOznacnik(Base):
    __tablename__ = "jdf_oznacniky"  # name of the table
    id = Column(Integer, Sequence("jdf_oznacniky_id"), primary_key=True)
    id_zastavka = Column(Integer)
    cislo_zastavka = Column(Integer)  # je treba?
    cislo_oznacnik = Column(Integer)  # je treba?
    nazev = Column(VARCHAR(48))
    smer = Column(VARCHAR(48))
    stanoviste = Column(VARCHAR(12))


class JdfDopravce(Base):
    __tablename__ = "jdf_dopravci"  # name of the table
    id = Column(Integer, Sequence("jdf_dopravci_id"), primary_key=True)
    ico = Column(VARCHAR(10))
    dic = Column(VARCHAR(14))
    jmeno = Column(VARCHAR(254))
    druh_firmy = Column(Integer)
    jmeno_osoby = Column(VARCHAR(254))
    sidlo = Column(VARCHAR(254))
    telefon_sidlo = Column(VARCHAR(48))
    telefon_dispecink = Column(VARCHAR(48))
    telefon_informace = Column(VARCHAR(48))
    fax = Column(VARCHAR(48))
    email = Column(VARCHAR(48))
    www = Column(VARCHAR(48))
    rozliseni = Column(Integer)


class JdfOdjezdy(Base):
    __tablename__ = "jdf_odjezdy"  # name of the table
    id = Column(Integer, Sequence("jdf_odjezdy_id"), primary_key=True)
    id_linka = Column(Integer)
    id_spoj = Column(Integer)
    id_zastavka = Column(Integer)
    id_oznacnik = Column(Integer)
    cislo_linka = Column(CHAR(6))
    cislo_spoj = Column(Integer)
    cislo_tarifni = Column(Integer)
    cislo_zastavka = Column(Integer)
    cislo_oznacnik = Column(Integer)
    cislo_stanoviste = Column(VARCHAR(48))
    km = Column(Integer)
    prijezd = Column(VARCHAR(5))
    odjezd = Column(VARCHAR(5))
    prijezd_min = Column(VARCHAR(5))
    odjezd_max = Column(VARCHAR(5))
    rozliseni = Column(Integer)


class JdfSpoj(Base):
    __tablename__ = "jdf_spoje"  # name of the table
    id = Column(Integer, Sequence("jdf_spoje_id"), primary_key=True)
    id_linka = Column(Integer)
    id_dopravce = Column(Integer)
    id_kalendar = Column(Integer)
    cislo_linka = Column(CHAR(6))
    cislo_spoj = Column(Integer)
    skupina_spoju = Column(Integer)
    rozl_linka = Column(Integer)


class JdfLinka(Base):
    __tablename__ = "jdf_linky"  # name of the table
    id = Column(Integer, Sequence("jdf_linky_id"), primary_key=True)
    id_dopravce = Column(Integer)
    cislo_linka = Column(CHAR(6))
    nazev_linka = Column(VARCHAR(254))
    ic_dopravce = Column(VARCHAR(10))
    typ_linka = Column(CHAR(1))
    dopravni_prostredek = Column(CHAR(1))
    objizdkovy_jr = Column(BOOLEAN, default=False)
    seskupeni = Column(BOOLEAN, default=False)
    oznacniky = Column(BOOLEAN, default=False)
    jednosmerny_jr = Column(BOOLEAN, default=False)
    cislo_licence = Column(VARCHAR(48))
    platnost_lic_od = Column(VARCHAR(10))
    platnost_lic_do = Column(VARCHAR(10))
    platnost_od = Column(VARCHAR(10))
    platnost_do = Column(VARCHAR(10))
    rozl_dopravce = Column(Integer)
    rozl_linka = Column(Integer)
    aktualni = Column(BOOLEAN, default=True)
    skip = Column(BOOLEAN, default=False)


class JdfKalendar(Base):
    __tablename__ = "jdf_kalendar"  # name of the table
    id = Column(Integer, Sequence("jdf_kalendar_id"), primary_key=True)
    od = Column(VARCHAR(10))
    do = Column(VARCHAR(10))
    po = Column(Integer)
    ut = Column(Integer)
    st = Column(Integer)
    ct = Column(Integer)
    pa = Column(Integer)
    so = Column(Integer)
    ne = Column(Integer)
    prac_den = Column(Integer)
    svatek = Column(Integer)


class JdfKalendarVyjimky(Base):
    __tablename__ = "jdf_kalendar_vyjimky"  # name of the table
    id = Column(Integer, Sequence("jdf_kalendar_vyjimka_id"), primary_key=True)
    datum = Column(TEXT)
    typ = Column(Integer)
    service_id = Column(Integer)


Base.metadata.create_all(engine)


class CzechCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('Nový rok', month=1, day=1),
        Holiday('Velký pátek', month=1, day=1, offset=[Easter(), Day(-2)]),
        Holiday('Velikonoční pondělí', month=1, day=1, offset=[Easter(), Day(1)]),
        Holiday('Svátek práce', month=5, day=1),
        Holiday('Den vítězství', month=5, day=8),
        Holiday('Den slovanských věrozvěstů Cyrila a Metoděje ', month=7, day=5),
        Holiday('Den upálení mistra Jana Husa ', month=7, day=6),
        Holiday('Den české státnosti', month=9, day=28),
        Holiday('Den vzniku samostatného československého státu', month=10, day=28),
        Holiday('Den boje za svobodu a demokracii', month=10, day=28),
        Holiday('Štědrý den', month=12, day=24),
        Holiday('1. svátek vánoční', month=12, day=25),
        Holiday('2. svátek vánoční', month=12, day=26)
    ]


def svatky_bez_nedel(start_date, end_date, even_odd):
    holidays = pd.to_datetime(CzechCalendar().holidays(start_date, end_date))
    final_dates = []
    for day in holidays:
        if day.dayofweek < 6:
            if even_odd is None:
                final_dates.append(day.strftime("%Y-%m-%d"))
            else:
                if (day.weekofyear % 2) == even_odd:
                    final_dates.append(day.strftime("%Y-%m-%d"))
    return final_dates


def svatky_pondeli_az_patek(start_date, end_date):
    holidays = pd.to_datetime(CzechCalendar().holidays(start_date, end_date))
    final_dates = []
    for day in holidays:
        if day.dayofweek < 5:
            final_dates.append(day.strftime("%Y-%m-%d"))
    return final_dates

def svatky_v_sobotu(start_date, end_date):
    holidays = pd.to_datetime(CzechCalendar().holidays(start_date, end_date))
    final_dates = []
    for day in holidays:
        if day.dayofweek == 5:
            final_dates.append(day.strftime("%Y-%m-%d"))
    return final_dates


class CasovyKod():
    def __init__(self, typ, od_kod, do_kod, linka, spoj):
        self.typ = typ
        self.od_kod = f'{od_kod[4:8]}-{od_kod[2:4]}-{od_kod[0:2]}'
        if od_kod == '':
            self.od_kod = ''
        else:
            self.od_kod = f'{od_kod[4:8]}-{od_kod[2:4]}-{od_kod[0:2]}'
        if do_kod == '':
            self.do_kod = ''
        else:
            self.do_kod = f'{do_kod[4:8]}-{do_kod[2:4]}-{do_kod[0:2]}'
        self.linka = linka
        self.spoj = spoj


def parse_calendar(od, do, prac_den, svatek, po, ut, st, ct, pa, so, ne, casovekody):
    jede_take, nejede = [], []
    use_calendar = True
    od = f'{od[4:8]}-{od[2:4]}-{od[0:2]}'
    do = f'{do[4:8]}-{do[2:4]}-{do[0:2]}'

    if prac_den == 1:
        po = 1
        ut = 1
        st = 1
        ct = 1
        pa = 1
    if svatek == 1:
        ne = 1
    if (prac_den == 0) and (svatek == 0) and (po == 0) and (ut == 0) and (st == 0) and (ct == 0) and (pa == 0) and (
            so == 0) and (ne == 0):
        po = 1
        ut = 1
        st = 1
        ct = 1
        pa = 1
        so = 1
        ne = 1

    if len(casovekody) > 0:  # TODO:cele zkusit predelat do custom business days
        if len(casovekody) == 1 and (casovekody[0].typ == '1'):
            od = casovekody[0].od_kod
            do = casovekody[0].do_kod
            if do == '':
                do = od
            use_calendar = True
        else:
            for casovykod in casovekody:
                if casovykod.typ == '1':
                    use_calendar = False
                    if casovykod.do_kod != '':
                        tmp = []
                        if po == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-MON').date]
                        if ut == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-TUE').date]
                        if st == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-WED').date]
                        if ct == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-THU').date]
                        if pa == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-FRI').date]
                        if so == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-SAT').date]
                        if ne == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-SUN').date]
                        if (prac_den == 1) and (svatek == 1):
                            jede_take = [*jede_take, *tmp]
                            for day in svatky_v_sobotu(casovykod.od_kod, casovykod.do_kod):
                                if str(day) not in jede_take:
                                    jede_take.append(day)
                        elif prac_den == 1:
                            svatky_v_prac_dny = svatky_pondeli_az_patek(casovykod.od_kod, casovykod.do_kod)
                            for day in tmp:
                                if str(day) not in svatky_v_prac_dny:
                                    jede_take.append(day)
                        elif svatek == 1:
                            jede_take = [*jede_take, *tmp]
                            for day in svatky_bez_nedel(casovykod.od_kod, casovykod.do_kod, None):
                                if str(day) not in jede_take:
                                    jede_take.append(day)
                        else:
                            jede_take = [*jede_take, *tmp]
                    else:
                        jede_take.append(casovykod.od_kod)
                elif casovykod.typ == '2':
                    jede_take.append(casovykod.od_kod)
                elif casovykod.typ == '3':
                    use_calendar = False
                    jede_take.append(casovykod.od_kod)
                elif casovykod.typ == '4':
                    if casovykod.do_kod != '':
                        tmp = []
                        if po == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-MON').date]
                        if ut == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-TUE').date]
                        if st == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-WED').date]
                        if ct == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-THU').date]
                        if pa == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-FRI').date]
                        if so == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-SAT').date]
                        if ne == 1:
                            tmp = [*tmp,
                                   *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-SUN').date]
                        if svatek == 1:
                            nejede = [*nejede, *tmp]
                            for day in svatky_bez_nedel(casovykod.od_kod, casovykod.do_kod, None):
                                if str(day) not in nejede:
                                    nejede.append(day)
                        else:
                            nejede = [*nejede, *tmp]
                    else:
                        nejede.append(casovykod.od_kod)
                elif casovykod.typ == '5':  # liche
                    use_calendar = False
                    tmp = []
                    if po == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-MON').array]
                    if ut == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-TUE').array]
                    if st == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-WED').array]
                    if ct == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-THU').array]
                    if pa == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-FRI').array]
                    if so == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-SAT').array]
                    if ne == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-SUN').array]
                    for i in tmp:
                        if (i.weekofyear % 2) == 1:
                            if prac_den == 1:
                                svatky_v_prac_dny = svatky_pondeli_az_patek(od, do)
                                if str(i._date_repr) not in svatky_v_prac_dny:
                                    jede_take.append(i._date_repr)
                            else:
                                jede_take.append(i._date_repr)
                    if svatek == 1:
                        for day in svatky_bez_nedel(od, do, 1):
                            if str(day) not in jede_take:
                                jede_take.append(day)
                elif casovykod.typ == '6':  # sude
                    use_calendar = False
                    tmp = []
                    if po == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-MON').array]
                    if ut == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-TUE').array]
                    if st == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-WED').array]
                    if ct == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-THU').array]
                    if pa == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-FRI').array]
                    if so == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-SAT').array]
                    if ne == 1:
                        tmp = [*tmp, *pd.date_range(start=od, end=do, freq='W-SUN').array]
                    for i in tmp:
                        if (i.weekofyear % 2) == 0:
                            if prac_den == 1:
                                svatky_v_prac_dny = svatky_pondeli_az_patek(od, do)
                                if str(i._date_repr) not in svatky_v_prac_dny:
                                    jede_take.append(i._date_repr)
                            else:
                                jede_take.append(i._date_repr)
                    if svatek == 1:
                        for day in svatky_bez_nedel(od, do, 0):
                            if str(day) not in jede_take:
                                jede_take.append(day)
                elif casovykod.typ == '7':  # liche
                    use_calendar = False
                    tmp = []
                    if po == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-MON').array]
                    if ut == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-TUE').array]
                    if st == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-WED').array]
                    if ct == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-THU').array]
                    if pa == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-FRI').array]
                    if so == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-SAT').array]
                    if ne == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-SUN').array]
                    for i in tmp:
                        if (i.weekofyear % 2) == 1:
                            if prac_den == 1:
                                svatky_v_prac_dny = svatky_pondeli_az_patek(casovykod.od_kod, casovykod.do_kod)
                                if str(i._date_repr) not in svatky_v_prac_dny:
                                    jede_take.append(i._date_repr)
                            else:
                                jede_take.append(i._date_repr)
                    if svatek == 1:
                        for day in svatky_bez_nedel(casovykod.od_kod, casovykod.do_kod, 1):
                            if str(day) not in jede_take:
                                jede_take.append(day)
                elif casovykod.typ == '8':  # sude
                    use_calendar = False
                    tmp = []
                    if po == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-MON').array]
                    if ut == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-TUE').array]
                    if st == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-WED').array]
                    if ct == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-THU').array]
                    if pa == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-FRI').array]
                    if so == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-SAT').array]
                    if ne == 1:
                        tmp = [*tmp, *pd.date_range(start=casovykod.od_kod, end=casovykod.do_kod, freq='W-SUN').date]
                    for i in tmp:
                        if (i.weekofyear % 2) == 0:
                            if prac_den == 1:
                                svatky_v_prac_dny = svatky_pondeli_az_patek(casovykod.od_kod, casovykod.do_kod)
                                if str(i._date_repr) not in svatky_v_prac_dny:
                                    jede_take.append(i._date_repr)
                            else:
                                jede_take.append(i._date_repr)
                    if svatek == 1:
                        for day in svatky_bez_nedel(casovykod.od_kod, casovykod.do_kod, 0):
                            if str(day) not in jede_take:
                                jede_take.append(day)
    if use_calendar:
        if prac_den == 1:
            for d in svatky_pondeli_az_patek(od, do):
                if str(d) not in jede_take:
                    nejede.append(d)

        if svatek == 1:
            for d in svatky_bez_nedel(od, do, None):
                if str(d) not in nejede:
                    jede_take.append(d)
    for d in nejede:
        if d in jede_take:
            jede_take.remove(d)


    jede_take = [str(i) for i in jede_take]
    nejede = [str(i) for i in nejede]

    for d in nejede:
        if d in jede_take:
            jede_take.remove(d)

    if not use_calendar:
        nejede = []

    jede_take = list(set(jede_take))
    jede_take.sort()
    nejede = list(set(nejede))
    nejede.sort()


    if use_calendar:
        possible_kalendar_id = []
        if dbsession.query(func.count(JdfKalendar.id)).filter(JdfKalendar.po == po,
                                                              JdfKalendar.ut == ut,
                                                              JdfKalendar.st == st,
                                                              JdfKalendar.ct == ct,
                                                              JdfKalendar.pa == pa,
                                                              JdfKalendar.so == so,
                                                              JdfKalendar.ne == ne,
                                                              JdfKalendar.od == od,
                                                              JdfKalendar.do == do,
                                                              JdfKalendar.prac_den == prac_den,
                                                              JdfKalendar.svatek == svatek
                                                              ).scalar() > 0:
            possible_kalendar_id = dbsession.query(JdfKalendar.id).filter(JdfKalendar.po == po,
                                                                          JdfKalendar.ut == ut,
                                                                          JdfKalendar.st == st,
                                                                          JdfKalendar.ct == ct,
                                                                          JdfKalendar.pa == pa,
                                                                          JdfKalendar.so == so,
                                                                          JdfKalendar.ne == ne,
                                                                          JdfKalendar.od == od,
                                                                          JdfKalendar.do == do,
                                                                          JdfKalendar.prac_den == prac_den,
                                                                          JdfKalendar.svatek == svatek
                                                                          ).all()

        ##jede_take - check v DB a get id, service_id
        if len(jede_take) > 0:
            if dbsession.query(func.count(JdfKalendarVyjimky.id)).filter(JdfKalendarVyjimky.typ == 1,
                                                                         JdfKalendarVyjimky.datum == (
                                                                                 ','.join(map(str, jede_take)))
                                                                         ).scalar() > 0:
                possible_kalendar_id = dbsession.query(JdfKalendarVyjimky.service_id).filter(
                    JdfKalendarVyjimky.typ == 1,
                    JdfKalendarVyjimky.datum == (','.join(map(str, jede_take))),
                    JdfKalendarVyjimky.service_id.in_(possible_kalendar_id)
                ).all()
            else:
                possible_kalendar_id = []
        else:
            exlude_ids = dbsession.query(JdfKalendarVyjimky.service_id).filter(JdfKalendarVyjimky.typ == 1).all()
            possible_kalendar_id = dbsession.query(JdfKalendar.id).filter(JdfKalendar.id.in_(possible_kalendar_id),
                                                                          JdfKalendar.id.notin_(exlude_ids)).all()

        ##nejede - check v DB a get id, service_id
        if len(nejede) > 0:
            if dbsession.query(func.count(JdfKalendarVyjimky.id)).filter(JdfKalendarVyjimky.typ == 2,
                                                                         JdfKalendarVyjimky.datum == (
                                                                                 ','.join(map(str, nejede)))
                                                                         ).scalar() > 0:
                possible_kalendar_id = dbsession.query(JdfKalendarVyjimky.service_id).filter(
                    JdfKalendarVyjimky.typ == 2,
                    JdfKalendarVyjimky.datum == (
                        ','.join(map(str, nejede))),
                    JdfKalendarVyjimky.service_id.in_(
                        possible_kalendar_id)
                ).all()
            else:
                possible_kalendar_id = []
        else:
            exlude_ids = dbsession.query(JdfKalendarVyjimky.service_id).filter(JdfKalendarVyjimky.typ == 2).all()
            possible_kalendar_id = dbsession.query(JdfKalendar.id).filter(JdfKalendar.id.in_(possible_kalendar_id),
                                                                          JdfKalendar.id.notin_(exlude_ids)).all()

        if len(possible_kalendar_id) == 1:
            id_kalendar = possible_kalendar_id[0]
        else:  ##insert vseho znovu
            jdfkalendar = JdfKalendar(po=po, ut=ut, st=st,
                                      ct=ct, pa=pa, so=so, ne=ne, od=od, do=do, prac_den=prac_den, svatek=svatek)
            dbsession.add(jdfkalendar)
            dbsession.flush()
            id_kalendar = jdfkalendar.id

            if len(jede_take) > 0:
                jdfkalendarvyjimka = JdfKalendarVyjimky(typ=1, datum=(','.join(map(str, jede_take))),
                                                        service_id=id_kalendar)
                dbsession.add(jdfkalendarvyjimka)
                dbsession.flush()
                id_kalendar_vyjimka = jdfkalendarvyjimka.id

            if len(nejede) > 0:
                jdfkalendarvyjimka = JdfKalendarVyjimky(typ=2, datum=(','.join(map(str, nejede))),
                                                        service_id=id_kalendar)
                dbsession.add(jdfkalendarvyjimka)
                dbsession.flush()
                id_kalendar_vyjimka = jdfkalendarvyjimka.id

    else:  # without calendar
        exlude_ids = dbsession.query(JdfKalendar.id).all()
        possible_kalendar_id = []
        found = False
        if len(jede_take) > 0:
            if dbsession.query(func.count(JdfKalendarVyjimky.service_id)).filter(JdfKalendarVyjimky.typ == 1,
                                                                                 JdfKalendarVyjimky.datum == (
                                                                                         ','.join(map(str, jede_take)))
                                                                                 ).scalar() > 0:
                possible_kalendar_id = dbsession.query(JdfKalendarVyjimky.service_id).filter(
                    JdfKalendarVyjimky.typ == 1,
                    JdfKalendarVyjimky.datum == (
                        ','.join(map(str, jede_take))),
                    JdfKalendarVyjimky.service_id.notin_(
                        exlude_ids)
                ).all()
                found = True
        else:
            exlude_ids += dbsession.query(JdfKalendarVyjimky.service_id).filter(JdfKalendarVyjimky.typ == 1).all()
            possible_kalendar_id = dbsession.query(JdfKalendarVyjimky.service_id).filter(
                JdfKalendarVyjimky.service_id.notin_(exlude_ids)
            ).all()

        ##nejede - check v DB a get id, service_id
        if len(nejede) > 0:
            if dbsession.query(func.count(JdfKalendarVyjimky.service_id)).filter(JdfKalendarVyjimky.typ == 2,
                                                                                 JdfKalendarVyjimky.datum == (
                                                                                         ','.join(map(str, nejede)))
                                                                                 ).scalar() > 0:
                possible_kalendar_id = dbsession.query(JdfKalendarVyjimky.service_id).filter(
                    JdfKalendarVyjimky.typ == 2,
                    JdfKalendarVyjimky.datum == (
                        ','.join(map(str, nejede))),
                    JdfKalendarVyjimky.service_id.in_(
                        possible_kalendar_id),
                    JdfKalendarVyjimky.service_id.notin_(
                        exlude_ids)
                ).all()
                found = True
        else:
            exlude_ids += dbsession.query(JdfKalendarVyjimky.service_id).filter(JdfKalendarVyjimky.typ == 2).all()
            possible_kalendar_id = dbsession.query(JdfKalendarVyjimky.service_id).filter(
                JdfKalendarVyjimky.service_id.notin_(exlude_ids),
                JdfKalendarVyjimky.service_id.in_(possible_kalendar_id)
            ).all()

        if (len(possible_kalendar_id) == 1) and found:
            id_kalendar = possible_kalendar_id[0][0]
        else:
            seq = Sequence('jdf_kalendar_id')
            id_kalendar = dbsession.execute(seq)

            if len(jede_take) > 0:
                jdfkalendarvyjimka = JdfKalendarVyjimky(typ=1, datum=(','.join(map(str, jede_take))),
                                                        service_id=id_kalendar)
                dbsession.add(jdfkalendarvyjimka)
                dbsession.flush()
                id_kalendar_vyjimka = jdfkalendarvyjimka.id

            if len(nejede) > 0:
                jdfkalendarvyjimka = JdfKalendarVyjimky(typ=2, datum=(','.join(map(str, nejede))),
                                                        service_id=id_kalendar)
                dbsession.add(jdfkalendarvyjimka)
                dbsession.flush()
                id_kalendar_vyjimka = jdfkalendarvyjimka.id

    dbsession.commit()
    return id_kalendar

def main():
    if os.path.exists(args.in_dir):
        rootdir = args.in_dir
    #row_count = calculaterows(rootdir)
    if args.cisjr:
        rootdir = args.in_dir
        #with closing(request.urlopen('ftp://ftp.cisjr.cz/JDF/JDF.zip')) as r:
        with closing(request.urlopen('ftp://ftp.cisjr.cz/draha/mestske/JDF.zip')) as r:
            with open(os.path.join(rootdir, 'JDF.zip'), 'wb') as f:
                shutil.copyfileobj(r, f)
            with zipfile.ZipFile(os.path.join(rootdir, 'JDF.zip'), "r") as zip_ref:
                zip_ref.extractall(rootdir)
            os.remove(os.path.join(rootdir, 'JDF.zip'))

    if args.zip or args.cisjr:
        rootdir = args.in_dir
        for subdir, dirs, files in os.walk(rootdir):
            for file in files:
                with zipfile.ZipFile(os.path.join(subdir, file), "r") as zip_ref:
                    dirName = file.split(".")[0]
                    zip_ref.extractall(os.path.join(rootdir, dirName))
                os.remove(os.path.join(subdir, file))

    for subdir, dirs, files in os.walk(rootdir):
        if (len(dirs) == 0) & (len(files) > 0):

    #for subdir, dirs, files in sorted(os.walk(rootdir)):
        #if (len(files) > 0):
            #print(subdir)

            caskody = {}
            pevnekody = {}
            file_name = find_sensitive_path(subdir, 'VerzeJDF.txt')
            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                csv_reader = csv.reader(f)
                for jdfver in csv_reader:
                    if jdfver[0] != '1.9' and jdfver[0] != '1.8':
                        jdfver[-1] = jdfver[-1][:-1]
                    if jdfver[0] == '1.9' or jdfver[0] == '1.8':
                        file_name = find_sensitive_path(subdir, 'Dopravci.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            dopravci = {}
                            dopravci_spoj = {}
                            for row in csv_reader:
                                if len(row) == 12:
                                    row[-1] = row[-1][:-1]
                                    if row[3] == '':
                                        row[3] = 1
                                    # if args.keep_history:
                                    if dbsession.query(func.count(JdfDopravce.id)).filter(JdfDopravce.ico == row[0],
                                                                                          JdfDopravce.jmeno == row[2],
                                                                                          JdfDopravce.sidlo == row[
                                                                                              5]).scalar() < 1:
                                        jdfdopravce = JdfDopravce(ico=row[0], dic=row[1], jmeno=row[2],
                                                                  druh_firmy=row[3], jmeno_osoby=row[4], sidlo=row[5],
                                                                  telefon_sidlo=row[6], telefon_dispecink=row[7],
                                                                  telefon_informace=row[8], fax=row[9], email=row[10],
                                                                  www=row[11])
                                        dbsession.add(jdfdopravce)
                                        dbsession.flush()
                                        dopravce_id = jdfdopravce.id
                                    else:
                                        dopravce_id = dbsession.query(JdfDopravce).filter(JdfDopravce.ico == row[0],
                                                                                          JdfDopravce.jmeno == row[2],
                                                                                          JdfDopravce.sidlo == row[
                                                                                              5]).scalar().id
                                    dopravci[int(row[0])] = dopravce_id
                                    # else:
                                    #     jdfdopravce = JdfDopravce(ico=row[0], dic=row[1], jmeno=row[2],
                                    #                               druh_firmy=row[3], jmeno_osoby=row[4], sidlo=row[5],
                                    #                               telefon_sidlo=row[6], telefon_dispecink=row[7],
                                    #                               telefon_informace=row[8], fax=row[9], email=row[10],
                                    #                               www=row[11])
                                    #     dbsession.add(jdfdopravce)
                            dbsession.commit()
                            # if not args.keep_history:
                            #     result = dbsession.query(JdfDopravce).all()
                            #     for r in (result):
                            #         dopravci[int(r.ico)] = r.id
                        file_name = find_sensitive_path(subdir, 'Altdop.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 13:
                                        row[-1] = row[-1][:-1]
                                        dopravci_spoj[int(f'{row[0]}{row[1]}')] = dopravci[int(row[2])]
                        file_name = find_sensitive_path(subdir, 'Caskody.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 8:
                                        row[-1] = row[-1][:-1]
                                        caskod = CasovyKod(row[4], row[5], row[6], row[0], row[1])
                                        if int(f'{row[0]}{row[1]}') not in caskody:
                                            caskody[int(f'{row[0]}{row[1]}')] = []
                                        caskody[int(f'{row[0]}{row[1]}')].append(caskod)
                        file_name = find_sensitive_path(subdir, 'Pevnykod.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 3:
                                        row[-1] = row[-1][:-1]
                                        if row[0] not in pevnekody:
                                            pevnekody[row[0]] = []
                                        pevnekody[row[0]] = row[1]
                        #####cas kody - kazdy ulozit do promenne caskody[linkaspoj] -> array of CasovyKod
                        file_name = find_sensitive_path(subdir, 'Linky.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            for row in csv_reader:
                                if len(row) == 10:
                                    row[-1] = row[-1][:-1]
                                    id_dopravce = dopravci[int(row[2])]
                                    dopravci_spoj[int(row[0])] = id_dopravce
                                    if args.keep_history:
                                        if dbsession.query(func.count(JdfLinka.id)).filter(
                                                JdfLinka.cislo_linka == row[0]).scalar() < 1:
                                            jdflinka = JdfLinka(cislo_linka=row[0], nazev_linka=row[1], ic_dopravce=row[2],
                                                                typ_linka=row[3], cislo_licence=row[5],
                                                                platnost_lic_od=row[6],
                                                                platnost_lic_do=row[7],
                                                                platnost_od=row[8],
                                                                platnost_do=row[9], id_dopravce=id_dopravce)
                                            dbsession.add(jdflinka)
                                        else:
                                            aktualni_linka = dbsession.query(JdfLinka).filter(
                                                JdfLinka.cislo_linka == row[0], JdfLinka.aktualni == True).scalar()
                                            if (((aktualni_linka.platnost_od == row[8]) and (
                                                    aktualni_linka.platnost_do == row[9])) or (
                                                    datetime.strptime(aktualni_linka.platnost_od,
                                                                      '%d%m%Y') > datetime.strptime(row[8], '%d%m%Y'))):
                                                aktualni = False
                                                dbsession.query(func.count(JdfLinka.id)).filter(
                                                    JdfLinka.cislo_linka == row[0], JdfLinka.aktualni == True).update(
                                                    {"skip": True})
                                            else:
                                                aktualni = True
                                                dbsession.query(func.count(JdfLinka.id)).filter(
                                                    JdfLinka.cislo_linka == row[0], JdfLinka.aktualni == True).update(
                                                    {"aktualni": False, "platnost_do": row[8]})
                                            dbsession.commit()
                                            dbsession.flush()
                                            jdflinka = JdfLinka(cislo_linka=row[0], nazev_linka=row[1], ic_dopravce=row[2],
                                                                typ_linka=row[3], cislo_licence=row[5],
                                                                platnost_lic_od=row[6],
                                                                platnost_lic_do=row[7],
                                                                platnost_od=row[8],
                                                                platnost_do=row[9], id_dopravce=id_dopravce,
                                                                aktualni=aktualni)
                                            dbsession.add(jdflinka)
                                    else:
                                        jdflinka = JdfLinka(cislo_linka=row[0], nazev_linka=row[1], ic_dopravce=row[2],
                                                            typ_linka=row[3], cislo_licence=row[5],
                                                            platnost_lic_od=row[6],
                                                            platnost_lic_do=row[7],
                                                            platnost_od=row[8],
                                                            platnost_do=row[9], id_dopravce=id_dopravce,
                                                            aktualni=True)
                                        dbsession.add(jdflinka)
                            dbsession.commit()
                            if not args.keep_history:
                                result = dbsession.query(JdfLinka).all()
                                linky = {}
                                for r in result:
                                    linky[r.cislo_linka] = {'id': r.id, 'od': r.platnost_od, 'do': r.platnost_do}

                        file_name = find_sensitive_path(subdir, 'Spoje.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            for row in csv_reader:
                                if len(row) == 12:
                                    row[-1] = row[-1][:-1]
                                    if args.keep_history:
                                        linka = dbsession.query(JdfLinka).filter(JdfLinka.cislo_linka == row[0],
                                                                             JdfLinka.aktualni == True).scalar()
                                    else:
                                        linka = None
                                        id_linka = linky[row[0]]['id']
                                        od = linky[row[0]]['od']
                                        do = linky[row[0]]['do']
                                    if linka != None:
                                        if linka.skip == False:
                                            if args.keep_history:
                                                id_linka = linka.id
                                                od = linka.platnost_od
                                                do = linka.platnost_do
                                        else:
                                            id_linka = None
                                    if id_linka != None:
                                        if (int(f'{row[0]}{row[1]}') in dopravci_spoj):
                                            id_dopravce = dopravci_spoj[int(f'{row[0]}{row[1]}')]
                                        else:
                                            id_dopravce = dopravci_spoj[int(row[0])]
                                        po, ut, st, ct, pa, so, ne, prac_den, svatek = 0, 0, 0, 0, 0, 0, 0, 0, 0
                                        pevnekody_spoj = []
                                        for k in row[2:11]:
                                            if k != '':
                                                pevnekody_spoj.append(pevnekody[k])
                                        if '1' in pevnekody_spoj:
                                            po = 1
                                        if '2' in pevnekody_spoj:
                                            ut = 1
                                        if '3' in pevnekody_spoj:
                                            st = 1
                                        if '4' in pevnekody_spoj:
                                            ct = 1
                                        if '5' in pevnekody_spoj:
                                            pa = 1
                                        if '6' in pevnekody_spoj:
                                            so = 1
                                        if '7' in pevnekody_spoj:
                                            ne = 1
                                        if 'X' in pevnekody_spoj:
                                            prac_den = 1
                                        if '+' in pevnekody_spoj:
                                            svatek = 1
                                        if (int(f'{row[0]}{row[1]}') in caskody):
                                            caskod = caskody[int(f'{row[0]}{row[1]}')]
                                        else:
                                            caskod = []
                                        id_kalendar = parse_calendar(od, do, prac_den, svatek, po, ut, st, ct, pa, so,
                                                                     ne,
                                                                     caskod)
                                        jdfspoj = JdfSpoj(cislo_linka=row[0], cislo_spoj=row[1], id_linka=id_linka,
                                                          id_dopravce=id_dopravce, id_kalendar=id_kalendar)
                                        dbsession.add(jdfspoj)
                            dbsession.commit()
                            if not args.keep_history:
                                result = dbsession.query(JdfSpoj).all()
                                spoje = {}
                                for r in result:
                                    if r.cislo_linka in spoje:
                                        spoje[r.cislo_linka][r.cislo_spoj] = r.id
                                    else:
                                        spoje[r.cislo_linka] = {}
                                        spoje[r.cislo_linka][r.cislo_spoj] = r.id

                        file_name = find_sensitive_path(subdir, 'Zastavky.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            zastavky = {}
                            for row in csv_reader:
                                if len(row) == 12:
                                    row[-1] = row[-1][:-1]
                                    if args.stopids:
                                        if dbsession.query(func.count(JdfZastavka.id)).filter(
                                                JdfZastavka.cislo_zastavka == row[0]).scalar() < 1:
                                            jdfzastavka = JdfZastavka(cislo_zastavka=row[0], obec=row[1],
                                                                      cast_obce=row[2],
                                                                      misto=row[3], blizka_obec=row[4], stat=row[5])
                                            dbsession.add(jdfzastavka)
                                            dbsession.flush()
                                            stop_id = jdfzastavka.id
                                        else:
                                            stop_id = dbsession.query(JdfZastavka).filter(
                                                JdfZastavka.cislo_zastavka == row[0]).scalar().id

                                    else:
                                        if dbsession.query(func.count(JdfZastavka.id)).filter(
                                                JdfZastavka.obec == row[1],
                                                JdfZastavka.cast_obce == row[2],
                                                JdfZastavka.misto == row[
                                                    3]).scalar() < 1:
                                            jdfzastavka = JdfZastavka(cislo_zastavka=row[0], obec=row[1],
                                                                      cast_obce=row[2],
                                                                      misto=row[3], blizka_obec=row[4], stat=row[5])
                                            dbsession.add(jdfzastavka)
                                            dbsession.flush()
                                            stop_id = jdfzastavka.id
                                        else:
                                            stop_id = dbsession.query(JdfZastavka).filter(JdfZastavka.obec == row[1],
                                                                                          JdfZastavka.cast_obce == row[
                                                                                              2],
                                                                                          JdfZastavka.misto == row[
                                                                                              3]).scalar().id
                                    # vytvorit pole cislo zastavky -> id zastavky
                                    zastavky[int(row[0])] = stop_id
                            dbsession.commit()

                        # file_name = find_sensitive_path(subdir, 'Zaslinky.txt')
                        # with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                        #     csv_reader = csv.reader(f)
                        #     for row in csv_reader:
                        #         if len(row) == 7:
                        #             row[-1] = row[-1][:-1]
                        #             # najit linku v DB
                        #             # najit zastavku v DB
                        #             jdfzaslinka = JdfLinkaZastavky(cislo_linka=row[0], cislo_tarifni=row[1],
                        #                                            cislo_zastavka=row[3])
                        #             #pevne kody
                        #             dbsession.add(jdfzaslinka)
                        #     dbsession.commit()
                        file_name = find_sensitive_path(subdir, 'Zasspoje.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            for row in csv_reader:
                                #writelevel(csv_reader.line_num)
                                if len(row) == 10:
                                    row[-1] = row[-1][:-1]
                                    if (row[8] != '<') & (row[9] != '<') & (row[8] != '|') & (row[9] != '|'):
                                        if args.keep_history:
                                            linka = dbsession.query(JdfLinka).filter(JdfLinka.cislo_linka == row[0],
                                                                                 JdfLinka.aktualni == True).scalar()
                                        else:
                                            id_linka = linky[row[0]]['id']
                                            id_spoj = spoje[row[0]][int(row[1])]
                                        if linka != None:
                                            if linka.skip == False:
                                                if args.keep_history:
                                                    id_linka = linka.id
                                                    spoj = dbsession.query(JdfSpoj).filter(JdfSpoj.id_linka == id_linka,
                                                                                          JdfSpoj.cislo_spoj == row[
                                                                                              1]).scalar()

                                                    if spoj != None:
                                                        id_spoj = spoj.id
                                                    else:
                                                        id_spoj = None
                                            else:
                                                id_linka = None
                                        else:
                                            id_linka = None

                                        if (id_linka!= None) and (id_spoj != None):
                                            id_zastavka = zastavky[int(row[3])]
                                            jdfodjezd = JdfOdjezdy(cislo_linka=row[0], cislo_spoj=row[1],
                                                                   cislo_tarifni=row[2],
                                                                   cislo_zastavka=row[3], cislo_stanoviste=row[4],
                                                                   prijezd=row[8], odjezd=row[9], id_linka=id_linka,
                                                                   id_spoj=id_spoj, id_zastavka=id_zastavka)
                                            if row[7] != '':
                                                jdfodjezd.km = row[7]
                                            dbsession.add(jdfodjezd)
                            dbsession.commit()





                    elif jdfver[0] == '1.10':
                        file_name = find_sensitive_path(subdir, 'Dopravci.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            dopravci = {}
                            dopravci_spoj = {}
                            for row in csv_reader:
                                if len(row) == 13:
                                    row[-1] = row[-1][:-1]
                                    if row[3] == '':
                                        row[3] = 1
                                    # if args.keep_history:
                                    if dbsession.query(func.count(JdfDopravce.id)).filter(JdfDopravce.ico == row[0],
                                                                                          JdfDopravce.jmeno == row[2],
                                                                                          JdfDopravce.sidlo == row[
                                                                                              5]).scalar() < 1:
                                        jdfdopravce = JdfDopravce(ico=row[0], dic=row[1], jmeno=row[2],
                                                                  druh_firmy=row[3], jmeno_osoby=row[4], sidlo=row[5],
                                                                  telefon_sidlo=row[6], telefon_dispecink=row[7],
                                                                  telefon_informace=row[8], fax=row[9], email=row[10],
                                                                  www=row[11], rozliseni=row[12])
                                        dbsession.add(jdfdopravce)
                                        dbsession.flush()
                                        dopravce_id = jdfdopravce.id
                                    else:
                                        dopravce_id = dbsession.query(JdfDopravce).filter(JdfDopravce.ico == row[0],
                                                                                          JdfDopravce.jmeno == row[2],
                                                                                          JdfDopravce.sidlo == row[
                                                                                              5]).scalar().id
                                    dopravci[int(f'{row[0]}{row[12]}')] = dopravce_id
                                    # else:
                                    #     jdfdopravce = JdfDopravce(ico=row[0], dic=row[1], jmeno=row[2],
                                    #                                       druh_firmy=row[3], jmeno_osoby=row[4], sidlo=row[5],
                                    #                                       telefon_sidlo=row[6], telefon_dispecink=row[7],
                                    #                                       telefon_informace=row[8], fax=row[9], email=row[10],
                                    #                                       www=row[11], rozliseni=row[12])
                                    #     dbsession.add(jdfdopravce)
                                        # vytvorit pole cislo zastavky -> id zastavky

                            dbsession.commit()
                            # if not args.keep_history:
                            #     result = dbsession.query(JdfDopravce).all()
                            #     for r in result:
                            #         if r.rozliseni == None:
                            #             rozliseni = ''
                            #         else:
                            #             rozliseni = r.rozliseni
                            #         dopravci[int(f'{r.ico}{rozliseni}')] = r.id

                        file_name = find_sensitive_path(subdir, 'Altdop.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 15:
                                        row[-1] = row[-1][:-1]
                                        dopravci_spoj[int(f'{row[0]}{row[1]}')] = dopravci[int(f'{row[2]}{row[13]}')]
                        file_name = find_sensitive_path(subdir, 'Caskody.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 9:
                                        row[-1] = row[-1][:-1]
                                        caskod = CasovyKod(row[4], row[5], row[6], row[0], row[1])
                                        if int(f'{row[0]}{row[1]}') not in caskody:
                                            caskody[int(f'{row[0]}{row[1]}')] = []
                                        caskody[int(f'{row[0]}{row[1]}')].append(caskod)

                        file_name = find_sensitive_path(subdir, 'Pevnykod.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 3:
                                        row[-1] = row[-1][:-1]
                                        if row[0] not in pevnekody:
                                            pevnekody[row[0]] = []
                                        pevnekody[row[0]] = row[1]
                        file_name = find_sensitive_path(subdir, 'Linky.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            for row in csv_reader:
                                if len(row) == 16:
                                    row[-1] = row[-1][:-1]
                                    id_dopravce = dopravci[int(f'{row[2]}{row[14]}')]
                                    dopravci_spoj[int(row[0])] = id_dopravce
                                    if args.keep_history:
                                        if dbsession.query(func.count(JdfLinka.id)).filter(
                                                JdfLinka.cislo_linka == row[0]).scalar() < 1:
                                            jdflinka = JdfLinka(cislo_linka=row[0], nazev_linka=row[1], ic_dopravce=row[2],
                                                                typ_linka=row[3], dopravni_prostredek=row[4],
                                                                objizdkovy_jr=bool(int(row[5])),
                                                                seskupeni=bool(int(row[6])), oznacniky=bool(int(row[7])),
                                                                cislo_licence=row[9],
                                                                platnost_lic_od=row[10], platnost_lic_do=row[11],
                                                                platnost_od=row[12], platnost_do=row[13],
                                                                rozl_dopravce=row[14],
                                                                rozl_linka=row[15], id_dopravce=id_dopravce)
                                            dbsession.add(jdflinka)
                                        else:
                                            aktualni_linka = dbsession.query(JdfLinka).filter(
                                                JdfLinka.cislo_linka == row[0], JdfLinka.aktualni == True).scalar()
                                            if (((aktualni_linka.platnost_od == row[12]) and (
                                                    aktualni_linka.platnost_do == row[13])) or (
                                                    datetime.strptime(aktualni_linka.platnost_od,
                                                                      '%d%m%Y') > datetime.strptime(row[12], '%d%m%Y'))):
                                                aktualni = False
                                                dbsession.query(func.count(JdfLinka.id)).filter(
                                                    JdfLinka.cislo_linka == row[0], JdfLinka.aktualni == True).update(
                                                    {"skip": True})
                                            else:
                                                aktualni = True
                                                dbsession.query(func.count(JdfLinka.id)).filter(
                                                    JdfLinka.cislo_linka == row[0], JdfLinka.aktualni == True).update(
                                                    {"aktualni": False, "platnost_do": row[12]})
                                            dbsession.commit()
                                            dbsession.flush()
                                            jdflinka = JdfLinka(cislo_linka=row[0], nazev_linka=row[1], ic_dopravce=row[2],
                                                                typ_linka=row[3], dopravni_prostredek=row[4],
                                                                objizdkovy_jr=bool(int(row[5])),
                                                                seskupeni=bool(int(row[6])), oznacniky=bool(int(row[7])),
                                                                cislo_licence=row[9],
                                                                platnost_lic_od=row[10], platnost_lic_do=row[11],
                                                                platnost_od=row[12], platnost_do=row[13],
                                                                rozl_dopravce=row[14],
                                                                rozl_linka=row[15], id_dopravce=id_dopravce,
                                                                aktualni=aktualni)
                                            dbsession.add(jdflinka)
                                    else:
                                        jdflinka = JdfLinka(cislo_linka=row[0], nazev_linka=row[1], ic_dopravce=row[2],
                                                                typ_linka=row[3], dopravni_prostredek=row[4],
                                                                objizdkovy_jr=bool(int(row[5])),
                                                                seskupeni=bool(int(row[6])), oznacniky=bool(int(row[7])),
                                                                cislo_licence=row[9],
                                                                platnost_lic_od=row[10], platnost_lic_do=row[11],
                                                                platnost_od=row[12], platnost_do=row[13],
                                                                rozl_dopravce=row[14],
                                                                rozl_linka=row[15], id_dopravce=id_dopravce,
                                                                aktualni=True)
                                        dbsession.add(jdflinka)
                            dbsession.commit()
                            if not args.keep_history:
                                result = dbsession.query(JdfLinka).all()
                                linky = {}
                                for r in result:
                                    linky[r.cislo_linka] = {'id': r.id, 'od': r.platnost_od, 'do': r.platnost_do}
                        file_name = find_sensitive_path(subdir, 'Spoje.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            for row in csv_reader:
                                if len(row) == 14:
                                    row[-1] = row[-1][:-1]
                                    id_linka = None
                                    if args.keep_history:
                                        linka = dbsession.query(JdfLinka).filter(JdfLinka.cislo_linka == row[0],
                                                                                 JdfLinka.rozl_linka == row[13],
                                                                                 JdfLinka.aktualni == True).scalar()
                                    else:
                                        linka = None
                                        id_linka = linky[row[0]]['id']
                                        od = linky[row[0]]['od']
                                        do = linky[row[0]]['do']

                                    if linka != None:
                                        if linka.skip == False:
                                            if args.keep_history:
                                                id_linka = linka.id
                                                od = linka.platnost_od
                                                do = linka.platnost_do
                                        else:
                                            id_linka = None
                                    if id_linka != None:
                                        po, ut, st, ct, pa, so, ne, prac_den, svatek = 0, 0, 0, 0, 0, 0, 0, 0, 0
                                        pevnekody_spoj = []
                                        for k in row[2:11]:
                                            if k != '':
                                                pevnekody_spoj.append(pevnekody[k])
                                        if '1' in pevnekody_spoj:
                                            po = 1
                                        if '2' in pevnekody_spoj:
                                            ut = 1
                                        if '3' in pevnekody_spoj:
                                            st = 1
                                        if '4' in pevnekody_spoj:
                                            ct = 1
                                        if '5' in pevnekody_spoj:
                                            pa = 1
                                        if '6' in pevnekody_spoj:
                                            so = 1
                                        if '7' in pevnekody_spoj:
                                            ne = 1
                                        if 'X' in pevnekody_spoj:
                                            prac_den = 1
                                        if '+' in pevnekody_spoj:
                                            svatek = 1
                                        if (int(f'{row[0]}{row[1]}') in caskody):
                                            caskod = caskody[int(f'{row[0]}{row[1]}')]
                                        else:
                                            caskod = []
                                        id_kalendar = parse_calendar(od, do, prac_den, svatek, po, ut, st, ct, pa, so,
                                                                     ne,
                                                                     caskod)

                                        if (int(f'{row[0]}{row[1]}') in dopravci_spoj):
                                            id_dopravce = dopravci_spoj[int(f'{row[0]}{row[1]}')]
                                        else:
                                            id_dopravce = dopravci_spoj[int(row[0])]
                                        jdfspoj = JdfSpoj(cislo_linka=row[0], cislo_spoj=row[1],
                                                          rozl_linka=row[13], id_linka=id_linka,
                                                          id_dopravce=id_dopravce, id_kalendar=id_kalendar)
                                        if row[12] != '':
                                            jdfspoj.skupina_spoju = row[12]
                                        dbsession.add(jdfspoj)
                            dbsession.commit()
                            if not args.keep_history:
                                result = dbsession.query(JdfSpoj).all()
                                spoje = {}
                                for r in result:
                                    if r.cislo_linka in spoje:
                                        spoje[r.cislo_linka][r.cislo_spoj] = r.id
                                    else:
                                        spoje[r.cislo_linka] = {}
                                        spoje[r.cislo_linka][r.cislo_spoj] = r.id
                        file_name = find_sensitive_path(subdir, 'Zastavky.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            zastavky = {}
                            for row in csv_reader:
                                if len(row) == 12:
                                    row[-1] = row[-1][:-1]
                                    if args.stopids:
                                        if dbsession.query(func.count(JdfZastavka.id)).filter(
                                                JdfZastavka.cislo_zastavka == row[0]).scalar() < 1:
                                            jdfzastavka = JdfZastavka(cislo_zastavka=row[0], obec=row[1],
                                                                      cast_obce=row[2],
                                                                      misto=row[3], blizka_obec=row[4], stat=row[5])
                                            dbsession.add(jdfzastavka)
                                            dbsession.flush()
                                            stop_id = jdfzastavka.id
                                        else:
                                            stop_id = dbsession.query(JdfZastavka).filter(
                                                JdfZastavka.cislo_zastavka == row[0]).scalar().id

                                    else:
                                        if dbsession.query(func.count(JdfZastavka.id)).filter(
                                                JdfZastavka.obec == row[1],
                                                JdfZastavka.cast_obce == row[2],
                                                JdfZastavka.misto == row[
                                                    3]).scalar() < 1:
                                            jdfzastavka = JdfZastavka(cislo_zastavka=row[0], obec=row[1],
                                                                      cast_obce=row[2],
                                                                      misto=row[3], blizka_obec=row[4], stat=row[5])
                                            dbsession.add(jdfzastavka)
                                            dbsession.flush()
                                            stop_id = jdfzastavka.id
                                        else:
                                            stop_id = dbsession.query(JdfZastavka).filter(JdfZastavka.obec == row[1],
                                                                                          JdfZastavka.cast_obce == row[
                                                                                              2],
                                                                                          JdfZastavka.misto == row[
                                                                                              3]).scalar().id
                                    # vytvorit pole cislo zastavky -> id zastavky
                                    zastavky[int(row[0])] = stop_id
                            dbsession.commit()
                        file_name = find_sensitive_path(subdir, 'Oznacniky.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 7:
                                        row[-1] = row[-1][:-1]
                                        # TODO kontrola duplicity oznacniku v DB
                                        # TODO najit v DB zastavku dle nazvu
                                        jdfoznacnik = JdfOznacnik(cislo_zastavka=row[0], cislo_oznacnik=row[1],
                                                                  nazev=row[2],
                                                                  smer=row[3], stanoviste=row[4])
                                        # TODO pevne kody
                                        dbsession.add(jdfoznacnik)
                                dbsession.commit()
                        # file_name = find_sensitive_path(subdir, 'Zaslinky.txt')
                        # with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                        #     csv_reader = csv.reader(f)
                        #     for row in csv_reader:
                        #         if len(row) == 9:
                        #             row[-1] = row[-1][:-1]
                        #             # najit linku v DB
                        #             jdfzaslinka = JdfLinkaZastavky(cislo_linka=row[0], cislo_tarifni=row[1],
                        #                                            tarifni_pasmo=row[2],
                        #                                            cislo_zastavka=row[3], prumerna_doba=row[4],
                        #                                            rozliseni=row[8])
                        #             # pevne kody
                        #             dbsession.add(jdfzaslinka)
                        #     dbsession.commit()

                        file_name = find_sensitive_path(subdir, 'Zasspoje.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            for row in csv_reader:
                                #writelevel(csv_reader.line_num)
                                if len(row) == 12:
                                    row[-1] = row[-1][:-1]
                                    if (row[9] != '<') & (row[10] != '<') & (row[9] != '|') & (row[10] != '|') & (not ((row[9] == '') & (row[10] == ''))):
                                        if args.keep_history:
                                            linka = dbsession.query(JdfLinka).filter(JdfLinka.cislo_linka == row[0],
                                                                                     JdfLinka.rozl_linka == row[11],
                                                                                     JdfLinka.aktualni == True).scalar()
                                        else:
                                            linka = None
                                            id_linka = linky[row[0]]['id']
                                            id_spoj = spoje[row[0]][int(row[1])]
                                        if linka != None:
                                            if linka.skip == False:
                                                if args.keep_history:
                                                    id_linka = linka.id
                                                    spoj = dbsession.query(JdfSpoj).filter(JdfSpoj.id_linka == id_linka,
                                                                                          JdfSpoj.cislo_spoj == row[
                                                                                              1]).scalar()

                                                    if spoj != None:
                                                        id_spoj = spoj.id
                                                    else:
                                                        id_spoj = None
                                            else:
                                                id_linka = None
                                        else:
                                            id_linka = None

                                        if (id_linka!= None) and (id_spoj != None):
                                            id_zastavka = zastavky[int(row[3])]
                                            jdfodjezd = JdfOdjezdy(cislo_linka=row[0], cislo_spoj=row[1],
                                                                   cislo_tarifni=row[2],
                                                                   cislo_zastavka=row[3], cislo_stanoviste=row[5],
                                                                   prijezd=row[9], odjezd=row[10], rozliseni=row[11],
                                                                   id_linka=id_linka, id_spoj=id_spoj,
                                                                   id_zastavka=id_zastavka)
                                            if row[4] != '':
                                                jdfodjezd.cislo_oznacnik = row[4]
                                            if row[8] != '':
                                                jdfodjezd.km = row[8]
                                            dbsession.add(jdfodjezd)
                            dbsession.commit()





                    elif jdfver[0] == '1.11':
                        file_name = find_sensitive_path(subdir, 'Dopravci.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            dopravci = {}
                            dopravci_spoj = {}
                            for row in csv_reader:
                                if len(row) == 13:
                                    row[-1] = row[-1][:-1]
                                    if row[3] == '':
                                        row[3] = 1
                                    # if args.keep_history:
                                    if dbsession.query(func.count(JdfDopravce.id)).filter(JdfDopravce.ico == row[0],
                                                                                          JdfDopravce.jmeno == row[2],
                                                                                          JdfDopravce.sidlo == row[
                                                                                              5]).scalar() < 1:
                                        jdfdopravce = JdfDopravce(ico=row[0], dic=row[1], jmeno=row[2],
                                                                  druh_firmy=row[3], jmeno_osoby=row[4], sidlo=row[5],
                                                                  telefon_sidlo=row[6], telefon_dispecink=row[7],
                                                                  telefon_informace=row[8], fax=row[9], email=row[10],
                                                                  www=row[11], rozliseni=row[12])
                                        dbsession.add(jdfdopravce)
                                        dbsession.flush()
                                        dopravce_id = jdfdopravce.id
                                    else:
                                        dopravce_id = dbsession.query(JdfDopravce).filter(JdfDopravce.ico == row[0],
                                                                                          JdfDopravce.jmeno == row[2],
                                                                                          JdfDopravce.sidlo == row[
                                                                                              5]).scalar().id
                                    dopravci[int(f'{row[0]}{row[12]}')] = dopravce_id
                                    # else:
                                    #     jdfdopravce = JdfDopravce(ico=row[0], dic=row[1], jmeno=row[2],
                                    #                                       druh_firmy=row[3], jmeno_osoby=row[4], sidlo=row[5],
                                    #                                       telefon_sidlo=row[6], telefon_dispecink=row[7],
                                    #                                       telefon_informace=row[8], fax=row[9], email=row[10],
                                    #                                       www=row[11], rozliseni=row[12])
                                    #     dbsession.add(jdfdopravce)
                            dbsession.commit()
                            # if not args.keep_history:
                            #     result = dbsession.query(JdfDopravce).all()
                            #     for r in result:
                            #         if r.rozliseni == None:
                            #             rozliseni = ''
                            #         else:
                            #             rozliseni = r.rozliseni
                            #         dopravci[int(f'{r.ico}{rozliseni}')] = r.id

                        file_name = find_sensitive_path(subdir, 'Altdop.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 15:
                                        row[-1] = row[-1][:-1]
                                        dopravci_spoj[int(f'{row[0]}{row[1]}')] = dopravci[int(f'{row[2]}{row[13]}')]
                        file_name = find_sensitive_path(subdir, 'Caskody.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 9:
                                        row[-1] = row[-1][:-1]
                                        caskod = CasovyKod(row[4], row[5], row[6], row[0], row[1])
                                        if int(f'{row[0]}{row[1]}') not in caskody:
                                            caskody[int(f'{row[0]}{row[1]}')] = []
                                        caskody[int(f'{row[0]}{row[1]}')].append(caskod)
                        file_name = find_sensitive_path(subdir, 'Pevnykod.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 3:
                                        row[-1] = row[-1][:-1]
                                        if row[0] not in pevnekody:
                                            pevnekody[row[0]] = []
                                        pevnekody[row[0]] = row[1]

                        file_name = find_sensitive_path(subdir, 'Linky.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            for row in csv_reader:
                                if len(row) == 17:
                                    row[-1] = row[-1][:-1]
                                    id_dopravce = dopravci[int(f'{row[2]}{row[15]}')]
                                    dopravci_spoj[int(row[0])] = id_dopravce
                                    if args.keep_history:
                                        if dbsession.query(func.count(JdfLinka.id)).filter(
                                                JdfLinka.cislo_linka == row[0]).scalar() < 1:
                                            jdflinka = JdfLinka(cislo_linka=row[0], nazev_linka=row[1], ic_dopravce=row[2],
                                                                typ_linka=row[3], dopravni_prostredek=row[4],
                                                                objizdkovy_jr=bool(int(row[5])),
                                                                seskupeni=bool(int(row[6])), oznacniky=bool(int(row[7])),
                                                                jednosmerny_jr=bool(int(row[8])),
                                                                cislo_licence=row[10], platnost_lic_od=row[11],
                                                                platnost_lic_do=row[12], platnost_od=row[13],
                                                                platnost_do=row[14],
                                                                rozl_dopravce=row[15], rozl_linka=row[16],
                                                                id_dopravce=id_dopravce)
                                            dbsession.add(jdflinka)
                                        else:
                                            aktualni_linka = dbsession.query(JdfLinka).filter(
                                                JdfLinka.cislo_linka == row[0], JdfLinka.aktualni == True).scalar()
                                            if (((aktualni_linka.platnost_od == row[13]) and (
                                                    aktualni_linka.platnost_do == row[14])) or (
                                                    datetime.strptime(aktualni_linka.platnost_od,
                                                                      '%d%m%Y') > datetime.strptime(row[13], '%d%m%Y'))):
                                                aktualni = False
                                                dbsession.query(func.count(JdfLinka.id)).filter(
                                                    JdfLinka.cislo_linka == row[0], JdfLinka.aktualni == True).update(
                                                    {"skip": True})
                                            else:
                                                aktualni = True
                                                dbsession.query(func.count(JdfLinka.id)).filter(
                                                    JdfLinka.cislo_linka == row[0], JdfLinka.aktualni == True).update(
                                                    {"aktualni": False, "platnost_do": row[13]})
                                            dbsession.commit()
                                            dbsession.flush()
                                            jdflinka = JdfLinka(cislo_linka=row[0], nazev_linka=row[1], ic_dopravce=row[2],
                                                                typ_linka=row[3], dopravni_prostredek=row[4],
                                                                objizdkovy_jr=bool(int(row[5])),
                                                                seskupeni=bool(int(row[6])), oznacniky=bool(int(row[7])),
                                                                jednosmerny_jr=bool(int(row[8])),
                                                                cislo_licence=row[10], platnost_lic_od=row[11],
                                                                platnost_lic_do=row[12], platnost_od=row[13],
                                                                platnost_do=row[14],
                                                                rozl_dopravce=row[15], rozl_linka=row[16],
                                                                id_dopravce=id_dopravce, aktualni=aktualni)
                                            dbsession.add(jdflinka)
                                    else:
                                        jdflinka = JdfLinka(cislo_linka=row[0], nazev_linka=row[1], ic_dopravce=row[2],
                                                                typ_linka=row[3], dopravni_prostredek=row[4],
                                                                objizdkovy_jr=bool(int(row[5])),
                                                                seskupeni=bool(int(row[6])), oznacniky=bool(int(row[7])),
                                                                jednosmerny_jr=bool(int(row[8])),
                                                                cislo_licence=row[10], platnost_lic_od=row[11],
                                                                platnost_lic_do=row[12], platnost_od=row[13],
                                                                platnost_do=row[14],
                                                                rozl_dopravce=row[15], rozl_linka=row[16],
                                                                id_dopravce=id_dopravce, aktualni=True)
                                        dbsession.add(jdflinka)
                            dbsession.commit()
                            if not args.keep_history:
                                result = dbsession.query(JdfLinka).all()
                                linky = {}
                                for r in result:
                                    linky[r.cislo_linka] = {'id': r.id, 'od': r.platnost_od, 'do': r.platnost_do}

                        file_name = find_sensitive_path(subdir, 'Spoje.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            for row in csv_reader:
                                if len(row) == 14:
                                    row[-1] = row[-1][:-1]
                                    id_linka = None
                                    if args.keep_history:
                                        linka = dbsession.query(JdfLinka).filter(JdfLinka.cislo_linka == row[0], JdfLinka.rozl_linka == row[13],
                                                                             JdfLinka.aktualni == True).scalar()
                                    else:
                                        linka = None
                                        id_linka = linky[row[0]]['id']
                                        od = linky[row[0]]['od']
                                        do = linky[row[0]]['do']
                                    if linka != None:
                                        if linka.skip == False:
                                            if args.keep_history:
                                                id_linka = linka.id
                                                od = linka.platnost_od
                                                do = linka.platnost_do
                                        else:
                                            id_linka = None
                                    if id_linka != None:
                                        po, ut, st, ct, pa, so, ne, prac_den, svatek = 0, 0, 0, 0, 0, 0, 0, 0, 0
                                        pevnekody_spoj = []
                                        for k in row[2:11]:
                                            if k != '':
                                                pevnekody_spoj.append(pevnekody[k])
                                        if '1' in pevnekody_spoj:
                                            po = 1
                                        if '2' in pevnekody_spoj:
                                            ut = 1
                                        if '3' in pevnekody_spoj:
                                            st = 1
                                        if '4' in pevnekody_spoj:
                                            ct = 1
                                        if '5' in pevnekody_spoj:
                                            pa = 1
                                        if '6' in pevnekody_spoj:
                                            so = 1
                                        if '7' in pevnekody_spoj:
                                            ne = 1
                                        if 'X' in pevnekody_spoj:
                                            prac_den = 1
                                        if '+' in pevnekody_spoj:
                                            svatek = 1
                                        if (int(f'{row[0]}{row[1]}') in caskody):
                                            caskod = caskody[int(f'{row[0]}{row[1]}')]
                                        else:
                                            caskod = []
                                        id_kalendar = parse_calendar(od, do, prac_den, svatek, po, ut, st, ct, pa, so,
                                                                     ne,
                                                                     caskod)
                                        if (int(f'{row[0]}{row[1]}') in dopravci_spoj):
                                            id_dopravce = dopravci_spoj[int(f'{row[0]}{row[1]}')]
                                        else:
                                            id_dopravce = dopravci_spoj[int(row[0])]
                                        jdfspoj = JdfSpoj(cislo_linka=row[0], cislo_spoj=row[1],
                                                          rozl_linka=row[13], id_linka=id_linka,
                                                          id_dopravce=id_dopravce, id_kalendar=id_kalendar)
                                        if row[12] != '':
                                            jdfspoj.skupina_spoju = row[12]
                                        dbsession.add(jdfspoj)
                            dbsession.commit()
                            if not args.keep_history:
                                result = dbsession.query(JdfSpoj).all()
                                spoje = {}
                                for r in result:
                                    if r.cislo_linka in spoje:
                                        spoje[r.cislo_linka][r.cislo_spoj] = r.id
                                    else:
                                        spoje[r.cislo_linka] = {}
                                        spoje[r.cislo_linka][r.cislo_spoj] = r.id

                        file_name = find_sensitive_path(subdir, 'Zastavky.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            zastavky = {}
                            for row in csv_reader:
                                if len(row) == 12:
                                    row[-1] = row[-1][:-1]
                                    if args.stopids:
                                        if dbsession.query(func.count(JdfZastavka.id)).filter(
                                                JdfZastavka.cislo_zastavka == row[0]).scalar() < 1:
                                            jdfzastavka = JdfZastavka(cislo_zastavka=row[0], obec=row[1],
                                                                      cast_obce=row[2],
                                                                      misto=row[3], blizka_obec=row[4], stat=row[5])
                                            dbsession.add(jdfzastavka)
                                            dbsession.flush()
                                            stop_id = jdfzastavka.id
                                        else:
                                            stop_id = dbsession.query(JdfZastavka).filter(
                                                JdfZastavka.cislo_zastavka == row[0]).scalar().id

                                    else:
                                        if dbsession.query(func.count(JdfZastavka.id)).filter(
                                                JdfZastavka.obec == row[1],
                                                JdfZastavka.cast_obce == row[2],
                                                JdfZastavka.misto == row[
                                                    3]).scalar() < 1:
                                            jdfzastavka = JdfZastavka(cislo_zastavka=row[0], obec=row[1],
                                                                      cast_obce=row[2],
                                                                      misto=row[3], blizka_obec=row[4], stat=row[5])
                                            dbsession.add(jdfzastavka)
                                            dbsession.flush()
                                            stop_id = jdfzastavka.id
                                        else:
                                            stop_id = dbsession.query(JdfZastavka).filter(JdfZastavka.obec == row[1],
                                                                                          JdfZastavka.cast_obce == row[
                                                                                              2],
                                                                                          JdfZastavka.misto == row[
                                                                                              3]).scalar().id

                                    # vytvorit pole cislo zastavky -> id zastavky
                                    zastavky[int(row[0])] = stop_id
                            dbsession.commit()
                        file_name = find_sensitive_path(subdir, 'Oznacniky.txt')
                        if file_name:
                            with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                                csv_reader = csv.reader(f)
                                for row in csv_reader:
                                    if len(row) == 7:
                                        row[-1] = row[-1][:-1]
                                        # TODO kontrola duplicity oznacniku v DB
                                        # TODO najit v DB zastavku dle nazvu
                                        jdfoznacnik = JdfOznacnik(cislo_zastavka=row[0], cislo_oznacnik=row[1],
                                                                  nazev=row[2],
                                                                  smer=row[3], stanoviste=row[4])
                                        # TODO pevne kody
                                        dbsession.add(jdfoznacnik)
                                dbsession.commit()
                        # file_name = find_sensitive_path(subdir, 'Zaslinky.txt')
                        # with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                        #     csv_reader = csv.reader(f)
                        #     for row in csv_reader:
                        #         if len(row) == 9:
                        #             row[-1] = row[-1][:-1]
                        #             #  najit linku v DB
                        #             jdfzaslinka = JdfLinkaZastavky(cislo_linka=row[0], cislo_tarifni=row[1],
                        #                                            tarifni_pasmo=row[2],
                        #                                            cislo_zastavka=row[3], prumerna_doba=row[4],
                        #                                            rozliseni=row[8])
                        #             #  pevne kody
                        #         dbsession.add(jdfzaslinka)
                        #     dbsession.commit()

                        file_name = find_sensitive_path(subdir, 'Zasspoje.txt')
                        with open(os.path.join(subdir, file_name), 'r', encoding='cp1250') as f:
                            csv_reader = csv.reader(f)
                            for row in csv_reader:
                                #writelevel(csv_reader.line_num)
                                if len(row) == 15:
                                    row[-1] = row[-1][:-1]
                                    if (row[10] != '<') & (row[11] != '<') & (row[10] != '|') & (row[11] != '|'):
                                        id_linka = None
                                        if args.keep_history:
                                            linka = dbsession.query(JdfLinka).filter(JdfLinka.rozl_linka == row[14],JdfLinka.cislo_linka == row[0], JdfLinka.aktualni == True).scalar()
                                        else:
                                            linka = None
                                            id_linka = linky[row[0]]['id']
                                            id_spoj = spoje[row[0]][int(row[1])]
                                        if linka != None:
                                            if linka.skip == False:
                                                if args.keep_history:
                                                    id_linka = linka.id
                                                    spoj = dbsession.query(JdfSpoj).filter(JdfSpoj.id_linka == id_linka,
                                                                                          JdfSpoj.cislo_spoj == row[
                                                                                              1]).scalar()
                                                    if spoj != None:
                                                        id_spoj = spoj.id
                                                    else:
                                                        id_spoj = None

                                            else:
                                                id_linka = None
                                        if (id_linka != None) and (id_spoj != None):
                                            id_zastavka = zastavky[int(row[3])]
                                            jdfodjezd = JdfOdjezdy(cislo_linka=row[0], cislo_spoj=row[1],
                                                                   cislo_tarifni=row[2],
                                                                   cislo_zastavka=row[3], cislo_stanoviste=row[5],
                                                                   prijezd=row[10], odjezd=row[11], rozliseni=row[14],
                                                                   id_linka=id_linka, id_spoj=id_spoj,
                                                                   id_zastavka=id_zastavka)
                                            if row[4] != '':
                                                jdfodjezd.cislo_oznacnik = row[4]
                                            if row[9] != '':
                                                jdfodjezd.km = row[9]
                                            if row[12] != '':
                                                jdfodjezd.prijezd_min = row[12]
                                            if row[13] != '':
                                                jdfodjezd.odjezd_max = row[13]
                                            # TODO pevne kody
                                            dbsession.add(jdfodjezd)
                            dbsession.commit()
            if args.cisjr:
                shutil.rmtree(subdir)

    if args.stoplocations:
        with open(args.stoplocations, 'r') as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                if args.stopids:
                    stop = JdfZastavkaPoloha(cislo_zastavka=row[0], x=row[1], y=row[2])
                else:
                    stop = JdfZastavkaPoloha(nazev=row[0], x=row[1], y=row[2])
                dbsession.add(stop)
            dbsession.commit()



    data = dbsession.execute("SELECT id as agency_id, jmeno || ', ' || sidlo as agency_name, 'http://' || www as agency_url, 'Europe/Prague' as agency_timezone, 'cs' as agency_lang, telefon_sidlo as agency_phone, email as agency_email from jdf_dopravci")
    with open(os.path.join(args.out_dir,'agency.txt'), 'w', encoding='utf8') as f:
        out = csv.writer(f, lineterminator='\n')
        out.writerow(list(data._metadata._keymap.keys()))
        for item in data:
            out.writerow((list(item._row)))
    data = dbsession.execute("SELECT id as service_id, po as monday, ut as tuesday, st as wednesday, ct as thursday, pa as friday, so as saturday, ne as sunday, replace(od, '-', '')  as start_date, replace(\"do\", '-', '') as end_date FROM jdf_kalendar")
    with open(os.path.join(args.out_dir,'calendar.txt'), 'w', encoding='utf8') as f:
        out = csv.writer(f, lineterminator='\n')
        out.writerow(list(data._metadata._keymap.keys()))
        for item in data:
            out.writerow((list(item._row)))
    data = dbsession.execute("SELECT service_id, replace(unnest(string_to_array(datum, ',')), '-', '') as \"date\", typ as exception_type FROM jdf_kalendar_vyjimky")
    with open(os.path.join(args.out_dir,'calendar_dates.txt'), 'w', encoding='utf8') as f:
        out = csv.writer(f, lineterminator='\n')
        out.writerow(list(data._metadata._keymap.keys()))
        for item in data:
            out.writerow((list(item._row)))
    data = dbsession.execute("SELECT id as route_id, id_dopravce as agency_id, cislo_linka as route_short_name, nazev_linka as route_long_name, case when dopravni_prostredek='E' then 0 when dopravni_prostredek='M' then 1 when dopravni_prostredek='L' then 6 when dopravni_prostredek='P' then 4 else 3 end as route_type FROM jdf_linky WHERE aktualni=TRUE")
    with open(os.path.join(args.out_dir,'routes.txt'), 'w', encoding='utf8') as f:
        out = csv.writer(f, lineterminator='\n')
        out.writerow(list(data._metadata._keymap.keys()))
        for item in data:
            out.writerow((list(item._row)))
    data = dbsession.execute("SELECT trip_id, arrival_time, departure_time, stop_id, ROW_NUMBER () OVER (PARTITION BY trip_id ORDER BY km, departure_time, arrival_time) as stop_sequence FROM (SELECT id_spoj as trip_id, CASE WHEN prijezd = '' THEN CASE WHEN odjezd = '' THEN '' ELSE CASE WHEN substring(odjezd from 1 for 1)::integer - substring(FIRST_VALUE(odjezd) OVER (PARTITION BY id_spoj ORDER BY case when MOD(cislo_spoj,2)=1 then cislo_tarifni end asc, case when MOD(cislo_spoj,2)=0 then cislo_tarifni end desc) from 1 for 1)::integer < 0 THEN (substring(odjezd from 1 for 2)::integer + 24)::char(2) ELSE substring(odjezd from 1 for 2) END || ':' || substring(odjezd from 3 for 2) || ':' || '00' END ELSE CASE WHEN substring(prijezd from 1 for 1)::integer - substring(FIRST_VALUE(odjezd) OVER (PARTITION BY id_spoj ORDER BY case when MOD(cislo_spoj,2)=1 then cislo_tarifni end asc, case when MOD(cislo_spoj,2)=0 then cislo_tarifni end desc) from 1 for 1)::integer < 0 THEN (substring(prijezd from 1 for 2)::integer + 24)::char(2) ELSE substring(prijezd from 1 for 2) END || ':' || substring(prijezd from 3 for 2) || ':' || '00' END as arrival_time, CASE WHEN odjezd = '' THEN CASE WHEN prijezd = '' THEN '' ELSE CASE WHEN substring(prijezd from 1 for 1)::integer - substring(FIRST_VALUE(odjezd) OVER (PARTITION BY id_spoj ORDER BY case when MOD(cislo_spoj,2)=1 then cislo_tarifni end asc, case when MOD(cislo_spoj,2)=0 then cislo_tarifni end desc) from 1 for 1)::integer < 0 THEN (substring(prijezd from 1 for 2)::integer + 24)::char(2) ELSE substring(prijezd from 1 for 2) END || ':' || substring(prijezd from 3 for 2) || ':' || '00' END ELSE CASE WHEN substring(odjezd from 1 for 1)::integer - substring(FIRST_VALUE(odjezd) OVER (PARTITION BY id_spoj ORDER BY case when MOD(cislo_spoj,2)=1 then cislo_tarifni end asc, case when MOD(cislo_spoj,2)=0 then cislo_tarifni end desc) from 1 for 1)::integer < 0 THEN (substring(odjezd from 1 for 2)::integer + 24)::char(2) ELSE substring(odjezd from 1 for 2) END || ':' || substring(odjezd from 3 for 2) || ':' || '00' END as departure_time, id_zastavka as stop_id, km FROM jdf_odjezdy JOIN jdf_linky ON jdf_odjezdy.id_linka = jdf_linky.id WHERE (odjezd <> '' OR prijezd <> '') AND aktualni = TRUE) t1")
    with open(os.path.join(args.out_dir,'stop_times.txt'), 'w', encoding='utf8') as f:
        out = csv.writer(f, lineterminator='\n')
        out.writerow(list(data._metadata._keymap.keys()))
        for item in data:
            out.writerow((list(item._row)))
    if args.stoplocations:
        if args.stopids:
            data = dbsession.execute("SELECT jdf_zastavky.id as stop_id, obec || ',' || cast_obce || ',' || misto as stop_name, COALESCE(jdf_zastavky_polohy.x, 0) as stop_lat, COALESCE(jdf_zastavky_polohy.y, 0) as stop_lon FROM jdf_zastavky LEFT JOIN jdf_zastavky_polohy ON jdf_zastavky.cislo_zastavka = jdf_zastavky_polohy.cislo_zastavka")
        else:
            data = dbsession.execute(
                "SELECT jdf_zastavky.id as stop_id, obec || ',' || cast_obce || ',' || misto as stop_name, COALESCE(jdf_zastavky_polohy.x, 0) as stop_lat, COALESCE(jdf_zastavky_polohy.y, 0) as stop_lon FROM jdf_zastavky LEFT JOIN jdf_zastavky_polohy ON (jdf_zastavky.obec || ',' || jdf_zastavky.cast_obce || ',' || jdf_zastavky.misto) = jdf_zastavky_polohy.nazev")
    else:
        data = dbsession.execute("SELECT id as stop_id, obec || ',' || cast_obce || ',' || misto as stop_name, 0 as stop_lat, 0 as stop_lon FROM jdf_zastavky")
    with open(os.path.join(args.out_dir,'stops.txt'), 'w', encoding='utf8') as f:
        out = csv.writer(f, lineterminator='\n')
        out.writerow(list(data._metadata._keymap.keys()))
        for item in data:
            out.writerow((list(item._row)))
    data = dbsession.execute("SELECT id_linka as route_id, id_kalendar as service_id, jdf_spoje.id as trip_id FROM jdf_spoje JOIN jdf_linky ON jdf_spoje.id_linka = jdf_linky.id WHERE aktualni=TRUE")
    with open(os.path.join(args.out_dir,'trips.txt'), 'w', encoding='utf8') as f:
        out = csv.writer(f, lineterminator='\n')
        out.writerow(list(data._metadata._keymap.keys()))
        for item in data:
            out.writerow((list(item._row)))
    with zipfile.ZipFile(os.path.join(args.out_dir, 'gtfs.zip'), 'w') as zip:
        # Add multiple files to the zip
        zip.write(os.path.join(args.out_dir, 'agency.txt'), 'agency.txt')
        zip.write(os.path.join(args.out_dir, 'calendar.txt'), 'calendar.txt')
        zip.write(os.path.join(args.out_dir, 'calendar_dates.txt'), 'calendar_dates.txt')
        zip.write(os.path.join(args.out_dir, 'routes.txt'), 'routes.txt')
        zip.write(os.path.join(args.out_dir, 'stop_times.txt'), 'stop_times.txt')
        zip.write(os.path.join(args.out_dir, 'stops.txt'), 'stops.txt')
        zip.write(os.path.join(args.out_dir, 'trips.txt'), 'trips.txt')



if __name__ == '__main__':
    main()
    dbsession.close()
    if not args.store_db_tables:
        Base.metadata.drop_all(engine)
