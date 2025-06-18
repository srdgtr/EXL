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

ini_config = configparser.ConfigParser()
ini_config.read(Path.home() / "bol_export_files.ini")
config_db = dict(
    drivername="mariadb",
    username=ini_config.get("database leveranciers", "user"),
    password=ini_config.get("database leveranciers", "password"),
    host=ini_config.get("database leveranciers", "host"),
    port=ini_config.get("database leveranciers", "port"),
    database=ini_config.get("database leveranciers", "database"),
)
engine = create_engine(URL.create(**config_db))
scraper_name = Path.cwd().name

winkel, wachtwoord, artikel, ean = (ini_config.get("excellent wsdl", "winkel"), ini_config.get("excellent wsdl", "wachtwoord"), "", "")
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
    huidige_voorraad_excellent = huidige_voorraad_excellent.map(
        lambda x: x if not isinstance(x, list) else x[0] if len(x) else ""
    ).assign(sku=lambda x: x["sku"].str.strip())
    return huidige_voorraad_excellent


voorraad_excellent = process_xml(verkrijgen_actuele_voorraad_excellent)

voorraad_excellent = voorraad_excellent.assign(
    stock=lambda x: pd.to_numeric(x["stock"], errors='coerce').clip(upper=15).astype(int),
    sku=scraper_name + voorraad_excellent["sku"].astype(str),
)

date_now = datetime.now().strftime("%c").replace(":", "-")

act_path = Path.home() / scraper_name / "actueel"
voorraad_excellent.to_csv(
    act_path / f"{scraper_name}_actueele_voorraad_{date_now}.csv",
    index=False,
    encoding="utf-8-sig",
)

engine.dispose()
