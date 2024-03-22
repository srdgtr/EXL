import configparser
import os
import sys
from datetime import datetime
from pathlib import Path

import dropbox
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

sys.path.insert(0, str(Path.cwd().parent))
from bol_export_file import get_file
from process_results.process_data import save_to_db, save_to_dropbox,save_to_dropbox_vendit

ini_config = configparser.ConfigParser(interpolation=None)
ini_config.read(Path.home() / "bol_export_files.ini")

scraper_name = Path.cwd().name
korting_percent = int(ini_config.get("stap 1 vaste korting", scraper_name.lower()).strip("%"))
date_now = datetime.now().strftime("%c").replace(":", "-")

producten = pd.read_csv(
    max(Path.cwd().glob(f"{scraper_name}_hand_*.csv"), key=os.path.getctime), sep=",", dtype={"ean": object}
)
voorraad = pd.read_csv(
    max(Path.cwd().glob(f"{scraper_name}_stock_*.csv"), key=os.path.getctime), sep=",", dtype={"stock1": object, "ean": object}
)

voorraad_info = (
    pd.merge(producten, voorraad, on="ean", how="left")
    .assign(
        stock=lambda x: x["stock1"].fillna(x["stock"]).fillna(0),
        ean=lambda x: x["ean"].fillna("").str.split('.').str[0],
        eigen_sku=lambda x: scraper_name + x["sku"],
        lk=lambda x: (korting_percent * x["price"] / 100).round(2),
        price=lambda x: (x["price"] - x["lk"]).round(2),
    )
    .drop(columns="stock1")
)

voorraad_info.to_csv(Path.cwd() / f"{scraper_name}_{date_now}_final.csv", index=False, encoding="utf-8-sig")

extra_columns = {'BTW code': 21,'Leverancier': scraper_name.lower()}
vendit = voorraad_info.assign(**extra_columns).rename(
    columns={
        "eigen_sku":"Product nummer",
        "ean" :"EAN nummer",
        "normal_price": "Inkoopprijs exclusief",
        "brand": "Merk",
        "price_going": "Verkoopprijs inclusief",
        "cat1":"Groep Niveau 1",
        "cat2" : "Groep Niveau 2",
        "info": "Product omschrijving",
    }
)

save_to_dropbox_vendit(vendit, scraper_name)

latest_file = max(Path.cwd().glob(f"{scraper_name}_*_final.csv"), key=os.path.getctime)
save_to_dropbox(latest_file, scraper_name)

product_info = voorraad_info.rename(
    columns={
        # "sku":"onze_sku",
        # "ean":"ean",
        "brand": "merk",
        "stock": "voorraad",
        "price": "inkoop_prijs",
        # :"promo_inkoop_prijs",
        # :"promo_inkoop_actief",
        "group" :"category",
        "price_going": "advies_prijs",
        "info": "omschrijving",
    }
).assign(onze_sku=lambda x: scraper_name + x["sku"], import_date=datetime.now())

save_to_db(product_info)