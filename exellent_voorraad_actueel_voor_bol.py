# -*- coding: utf-8 -*-

##python -mzeep ('http://www.electrocentrale.be/bestellen/bestellen.asmx?WSDL') om overzicht van aangeboden soap te krijgen

from zeep import Client
import numpy as np
from datetime import datetime
import time
import lxml.etree as et
import pandas as pd
from sqlalchemy import create_engine

from sqlalchemy.engine.url import URL
import configparser
from pathlib import Path

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")
config_db = dict(
    drivername="mariadb",
    username=alg_config.get("database leveranciers", "user"),
    password=alg_config.get("database leveranciers", "password"),
    host=alg_config.get("database leveranciers", "host"),
    port=alg_config.get("database leveranciers", "port"),
    database=alg_config.get("database leveranciers", "database"),
)
engine = create_engine(URL.create(**config_db))

winkel, wachtwoord, artikel, ean = (alg_config.get("excellent wsdl", "winkel"), alg_config.get("excellent wsdl", "wachtwoord"), "", "")
client = Client("http://www.electrocentrale.be/bestellen/bestellen.asmx?WSDL")


def get_huidige_voorraad():
    return client.service.getStockRealTimeXML(winkel, wachtwoord, artikel, ean)


verkrijgen_actuele_voorraad_excellent = get_huidige_voorraad()
if len(verkrijgen_actuele_voorraad_excellent) < 10:
    time.sleep(60)
    verkrijgen_actuele_voorraad_excellent = get_huidige_voorraad()


def process_xml(verkrijgen_actuele_voorraad_excellent):
    parse_xml = et.ElementTree(et.fromstring(verkrijgen_actuele_voorraad_excellent)).getroot()
    huidige_voorraad_excellent = []

    for node in parse_xml.iter("ARTIKEL"):
        sku = node.xpath(".//ARTIKELNUMMER/text()")
        ean = node.xpath(".//ARTIKELEAN/text()")
        stock = node.xpath(".//INSTOCK/text()")
        huidige_voorraad_excellent.append([sku, ean, stock])

    dfcols_stock = ["sku", "ean", "stock"]
    huidige_voorraad_excellent = pd.DataFrame(huidige_voorraad_excellent, columns=dfcols_stock)
    huidige_voorraad_excellent = huidige_voorraad_excellent.applymap(
        lambda x: x if not isinstance(x, list) else x[0] if len(x) else ""
    )
    return huidige_voorraad_excellent


voorraad_excellent = process_xml(verkrijgen_actuele_voorraad_excellent)


voorraad_excellent = voorraad_excellent.assign(
    stock=lambda x: np.where(
        pd.to_numeric(x["stock"], errors="coerce") > 6, 6, x["stock"]
    ),  # om riciso te beperken max 6
    sku="EXL" + voorraad_excellent["sku"].astype(str),
)
date_now = datetime.now().strftime("%c").replace(":", "-")

exl_act_path = Path.home() / "EXL" / "actueel"
voorraad_excellent.to_csv(
    exl_act_path / f"EXL_actueele_voorraad_{date_now}.csv",
    index=False,
    encoding="utf-8-sig",
)

voorraad_excellent_db = voorraad_excellent[
    [
        "sku",
        "ean",
        "stock",
    ]
]

voorraad_excellent_db.to_sql("EXL_voorraad", con=engine, if_exists="replace", index=False, chunksize=1000)


with engine.connect() as con:

    aantal_items = con.execute("SELECT count(*) FROM EXL_voorraad").fetchall()[-1][-1]
    totaal_stock = int(con.execute("SELECT sum(stock) FROM EXL_voorraad").fetchall()[-1][-1])
    leverancier = "EXL_voorraad"
    sql_insert = "INSERT INTO process_import_log_voorraad (aantal_items, totaal_stock, leverancier) VALUES (%s,%s,%s)"
    con.execute(sql_insert, (aantal_items, totaal_stock, leverancier))

engine.dispose()
