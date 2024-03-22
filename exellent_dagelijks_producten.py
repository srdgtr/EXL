# -*- coding: utf-8 -*-

##python -mzeep ('http://www.electrocentrale.be/bestellen/bestellen.asmx?WSDL') om overzicht van aangeboden soap te krijgen

from sqlalchemy import URL, create_engine
from zeep import Client
from zipfile import ZipFile
import os
from datetime import datetime, time, timedelta
import lxml.etree as et
import pandas as pd
import numpy as np
import dropbox
from pathlib import Path
import configparser
ini_config = configparser.ConfigParser(interpolation=None)
ini_config.read(Path.home() / "bol_export_files.ini")
dbx = dropbox.Dropbox(os.environ.get("DROPBOX"))
date_now = datetime.now().strftime("%c").replace(":", "-")
url = URL.create(**ini_config["database odin alchemy"])
engine = create_engine(url)
winkel, wachtwoord, artikel, ean = (ini_config.get("excellent wsdl", "winkel"), ini_config.get("excellent wsdl", "wachtwoord"), "", "")
client = Client("http://www.electrocentrale.be/bestellen/bestellen.asmx?WSDL")
scraper_name = Path.cwd().name

def get_voorraad():
    return client.service.getVoorraadzip(winkel, wachtwoord, artikel, ean)


getvoorraad = get_voorraad()
if len(getvoorraad) < 10:
    time.sleep(60)
    getvoorraad = get_voorraad()

with open(Path.cwd() / f"excellent {date_now}.zip", mode="wb") as f:
    f.write(getvoorraad)
voorraad = max(Path.cwd().glob("excellent*.zip"), key=os.path.getctime)
with ZipFile(voorraad, "r") as zip:
    zip.extractall(Path.cwd())
    latest_xml_file = max(Path.cwd().glob("*.xml"), key=os.path.getctime)
    os.rename(latest_xml_file, Path.cwd() / f"downloaded_xml_excellent {date_now}.xml")

parser = et.XMLParser(recover=True) #sometimes malformed xml charachters

def process_file():
    """main"""
    parsed_xml = et.parse(str(max(Path.cwd().glob("downloaded_xml_*.xml"), key=os.path.getctime)), parser=parser).getroot()
    dfcols = ["sku", "ean", "brand", "id", "group", "normal_price", "price_going", "stock", "info","promo_price","korting","korting2","korting3","korting4","promo_van","promo_tot","auteur_recht1","auteur_recht2","verwijder_bijdrage","disconto","staffel","staffel_tot_datum","staffel_aantal","staffel_percentage","cat1","cat2"]
    df_xml = []

    for node in parsed_xml.iter("Table"):
        sku = node.xpath(".//ARTIKEL/text()")
        ean = node.xpath(".//EAN/text()")
        brand = node.xpath(".//OMSCHRIJVING/text()")
        id = node.xpath(".//PARTNR/text()")
        group = node.xpath(".//SUBGROEP2/text()")
        normal_price = node.xpath(".//UITGEREKENDENETTOAANKOOPPRIJS/text()")
        price_going = node.xpath(".//PUBLIEKSPRIJS/text()")
        info = node.xpath(".//OMSCHRIJVINGNL/text()")
        stock = node.xpath(".//STOCK/text()")
        promo_price = node.xpath(".//UITGEREKENDEPROMOTIEPRIJS/text()")
        korting = node.xpath(".//KORTING1/text()")
        korting2 = node.xpath(".//KORTING2/text()")
        korting3 = node.xpath(".//KORTING3/text()")
        korting4 = node.xpath(".//KORTING4/text()")
        promo_van = [str(s) for s in node.xpath(".//PROMOTIEVAN/text()")] 
        promo_tot = [str(s) for s in node.xpath(".//PROMOTIETOT/text()")]
        disconto = node.xpath(".//DISCONTO/text()")
        auteur_recht1 = node.xpath(".//AUTEURSRECHTEN1/text()")
        auteur_recht2 = node.xpath(".//AUTEURSRECHTEN2/text()")
        verwijder_bijdrage = node.xpath(".//RECUPEL/text()")
        staffel = node.xpath(".//STAFFELYN/text()")
        staffel_tot_datum = node.xpath(".//TOTDATUM/text()")
        staffel_aantal = node.xpath(".//AANTAL1/text()")
        staffel_percentage = node.xpath(".//PERCENTAGE1/text()")
        cat1 = node.xpath(".//SUBGROEP1/text()")
        cat2 = node.xpath(".//SUBGROEP2/text()")

        df_xml.append([sku, ean, brand, id, group, normal_price, price_going, stock, info,promo_price,korting,korting2,korting3,korting4,promo_van,promo_tot,auteur_recht1,auteur_recht2,verwijder_bijdrage,disconto,staffel,staffel_tot_datum,staffel_aantal,staffel_percentage,cat1,cat2])
    df_xml = pd.DataFrame(df_xml, columns=dfcols)
    df_xml = (
        df_xml.map(lambda x: x if not isinstance(x, list) else x[0] if len(x) else "")
        .assign(
            stock=lambda x: pd.to_numeric(x["stock"], errors="coerce"),
            ean=lambda x: pd.to_numeric(x["ean"], errors="coerce"),
            normal_price=lambda x: pd.to_numeric(x["normal_price"], errors="coerce"),
            auteursrecht = lambda x: (pd.to_numeric(x["auteur_recht1"], errors="coerce") + pd.to_numeric(x["auteur_recht2"], errors="coerce"))*0.75, # vanwege btw etc.
            promo_price=lambda x: pd.to_numeric(x["promo_price"], errors="coerce").replace(0, np.nan),
            promo_van = lambda x: pd.to_datetime(x["promo_van"], format='%Y%m%d', errors='coerce'),
            promo_tot = lambda x: pd.to_datetime(x["promo_tot"], format='%Y%m%d', errors='coerce'),
            promo_active = lambda x: np.where((x["promo_tot"] - timedelta(days=1)) >= datetime.now(), "yes","no"),
            price = lambda x: np.where(((x["promo_active"] == "yes") & (x["promo_price"].notnull())), x["promo_price"]+x["auteursrecht"],x["normal_price"]+x["auteursrecht"]),
        )
        .query("ean == ean")
    )
    return df_xml
    
result_all = process_file()

result_all.dropna(subset=['promo_tot']).to_csv(f"{scraper_name}_{date_now}_acties.csv", index=False, encoding="utf-8-sig")

result = result_all.query("stock > 0")

result.to_csv(Path.cwd() / f"{scraper_name}_hand_{date_now}.csv", index=False, encoding="utf-8-sig")

result[["sku","ean"]].to_sql(name='ean_to_product_id_exl', con=engine, if_exists='replace',)

result.query("promo_active == 'yes'").to_csv(Path.cwd() / f"{scraper_name}_aanbiedingen_{date_now}.csv", index=False, encoding="utf-8-sig")
with open(f"{scraper_name}_aanbiedingen_{date_now}.csv", "rb") as f:
    dbx.files_upload(
        f.read(), f"/macro/datafiles/{scraper_name}_aanbiedingen_{date_now}.csv", mode=dropbox.files.WriteMode("overwrite", None)
    )

