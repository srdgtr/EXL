# -*- coding: utf-8 -*-

##python -mzeep ('http://www.electrocentrale.be/bestellen/bestellen.asmx?WSDL') om overzicht van aangeboden soap te krijgen

from zeep import Client
import lxml.etree as et
import pandas as pd
import numpy as np
from datetime import datetime
import configparser
from pathlib import Path

ini_config = configparser.ConfigParser()
ini_config.read(Path.home() / "bol_export_files.ini")
date_now = datetime.now().strftime("%c").replace(":", "-")

winkel, wachtwoord, artikel, ean = (
    ini_config.get("excellent wsdl", "winkel"),
    ini_config.get("excellent wsdl", "wachtwoord"),
    "",
    "",
)
client = Client("http://www.electrocentrale.be/bestellen/bestellen.asmx?WSDL")


def get_huidige_voorraad():
    return client.service.getStockRealTimeXML(winkel, wachtwoord, artikel, ean)


verkrijgen_actuele_voorraad_excellent = get_huidige_voorraad()


def process_xml(verkrijgen_actuele_voorraad_excellent):
    parse_xml = et.ElementTree(et.fromstring(verkrijgen_actuele_voorraad_excellent)).getroot()
    huidige_voorraad_excellent = []

    for node in parse_xml.iter("ARTIKEL"):
        ean = node.xpath(".//ARTIKELEAN/text()")
        stock1 = node.xpath(".//INSTOCK/text()")
        huidige_voorraad_excellent.append([ean, stock1])

    dfcols_stock = ["ean", "stock1"]
    huidige_voorraad_excellent = pd.DataFrame(huidige_voorraad_excellent, columns=dfcols_stock)
    huidige_voorraad_excellent = (
        huidige_voorraad_excellent.map(lambda x: x if not isinstance(x, list) else x[0] if len(x) else "")
        .drop_duplicates(subset=["ean"])
        .assign(stock1=lambda x: pd.to_numeric(x["stock1"], errors="coerce"))
    )
    return huidige_voorraad_excellent


voorraad_excellent = process_xml(verkrijgen_actuele_voorraad_excellent)

voorraad_excellent["stock1"] = np.where(
    voorraad_excellent["stock1"] > 6, 6, voorraad_excellent["stock1"]
)  # om riciso te beperken max 6
voorraad_excellent.to_csv(Path.cwd() / f"EXL_stock_{date_now}.csv", index=False)
