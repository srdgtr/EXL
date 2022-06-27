# -*- coding: utf-8 -*-

##python -mzeep ('http://www.electrocentrale.be/bestellen/bestellen.asmx?WSDL') om overzicht van aangeboden soap te krijgen

from zeep import Client
from zipfile import ZipFile
import os
from datetime import datetime, time
import lxml.etree as et
import pandas as pd

import configparser
from pathlib import Path

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")

date_now = datetime.now().strftime("%c").replace(":", "-")

winkel, wachtwoord, artikel, ean = (alg_config.get("excellent wsdl", "winkel"), alg_config.get("excellent wsdl", "wachtwoord"), "", "")
client = Client("http://www.electrocentrale.be/bestellen/bestellen.asmx?WSDL")


def get_voorraad():
    return client.service.getVoorraadzip(winkel, wachtwoord, artikel, ean)


getvoorraad = get_voorraad()
if len(getvoorraad) < 10:
    time.sleep(60)
    getvoorraad = get_voorraad()

exl_path = Path.home() / "EXL"

with open(exl_path / "temp" / f"excellent {date_now}.zip", mode="wb") as f:
    f.write(getvoorraad)
voorraad = max(exl_path.glob("**/excellent*.zip"), key=os.path.getctime)
with ZipFile(voorraad, "r") as zip:
    zip.extractall(exl_path / "temp")
    latest_xml_file = max(exl_path.glob("**/*.xml"), key=os.path.getctime)
    os.rename(latest_xml_file, exl_path / "temp"/ f"excellent {date_now}.xml")


def main():
    """main"""
    parsed_xml = et.parse(str(max(exl_path.glob("**/*.xml"), key=os.path.getctime))).getroot()
    dfcols = ["sku", "ean", "brand", "id", "group", "price", "price_going", "stock", "info"]
    df_xml = []

    for node in parsed_xml.iter("Table"):
        sku = node.xpath(".//ARTIKEL/text()")
        ean = node.xpath(".//EAN/text()")
        brand = node.xpath(".//OMSCHRIJVING/text()")
        id = node.xpath(".//PARTNR/text()")
        group = node.xpath(".//SUBGROEP2/text()")
        price = node.xpath(".//UITGEREKENDENETTOAANKOOPPRIJS/text()")
        price_going = node.xpath(".//PUBLIEKSPRIJS/text()")
        info = node.xpath(".//OMSCHRIJVINGNL/text()")
        stock = node.xpath(".//STOCK/text()")

        df_xml.append([sku, ean, brand, id, group, price, price_going, stock, info])
    df_xml = pd.DataFrame(df_xml, columns=dfcols)
    df_xml = (
        df_xml.applymap(lambda x: x if not isinstance(x, list) else x[0] if len(x) else "")
        .assign(
            stock=lambda x: pd.to_numeric(x["stock"], errors="coerce"),
            ean=lambda x: pd.to_numeric(x["ean"], errors="coerce"),
        )
        .query("stock > 0")
        .query("ean == ean")
    )
    df_xml.to_csv(exl_path / f"EXL_hand_{date_now}.csv", index=False, encoding="utf-8-sig")


main()
