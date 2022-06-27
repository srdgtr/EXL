import configparser
import os
import sys
from datetime import datetime
from pathlib import Path

import dropbox
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

sys.path.insert(0, str(Path.home()))
from bol_export_file import get_file

alg_config = configparser.ConfigParser()
alg_config.read(Path.home() / "general_settings.ini")
dbx_api_key = alg_config.get("dropbox", "api_dropbox")
dbx = dropbox.Dropbox(dbx_api_key)
config_db = dict(
    drivername="mariadb",
    username=alg_config.get("database leveranciers", "user"),
    password=alg_config.get("database leveranciers", "password"),
    host=alg_config.get("database leveranciers", "host"),
    port=alg_config.get("database leveranciers", "port"),
    database=alg_config.get("database leveranciers", "database"),
)
engine = create_engine(URL.create(**config_db))
current_folder = Path.cwd().name.upper()
export_config = configparser.ConfigParser(interpolation=None)
export_config.read(Path.home() / "bol_export_files.ini")
korting_percent = int(export_config.get("stap 1 vaste korting", current_folder.lower()).strip("%"))

exl_path = Path.home() / "EXL"
excellent_producten = pd.read_csv(
    max(exl_path.glob("EXL_hand_*.csv"), key=os.path.getctime), sep=",", dtype={"ean": object}
)
excellent_voorraad = pd.read_csv(
    max(exl_path.glob("EXL_stock_*.csv"), key=os.path.getctime), sep=",", dtype={"stock1": object, "ean": object}
)

excellent_merged = (
    pd.merge(excellent_producten, excellent_voorraad, on="ean", how="left")
    .assign(
        stock=lambda x: x["stock1"].fillna(x["stock"]).fillna(0),
        ean=lambda x: x["ean"].apply(str).apply(lambda x: x.zfill(15)).str.replace("000000000000nan", ""),
        eigen_sku=lambda x: "EXL" + x["sku"],
        gewicht="",
        lange_omschrijving="",
        url_artikel="",
        url_plaatje="",
        verpakings_eenheid="",
        lk=lambda x: (korting_percent * x["price"] / 100).round(2),
        price=lambda x: (x["price"] - x["lk"]).round(2),
    )
    .drop(columns="stock1")
)

date_now = datetime.now().strftime("%c").replace(":", "-")
excellent_merged.to_csv(exl_path / f"EXL_{date_now}_MATCHED.csv", index=False, encoding="utf-8-sig")


exl_info = excellent_merged.rename(
    columns={
        "price": "prijs",
        "brand": "merk",
        "group": "category",
        "info": "product_title",
        "eigenschappen": "lange_omschrijving",
        "Afbeelding": "url_plaatje",
        "stock": "voorraad",
        "price_going": "advies_prijs",
    }
)

latest_excelent_file = max(Path.cwd().glob("EXL_*_MATCHED.csv"), key=os.path.getctime)
with open(latest_excelent_file, "rb") as f:
    dbx.files_upload(
        f.read(), "/macro/datafiles/EXL/" + latest_excelent_file.name, mode=dropbox.files.WriteMode("overwrite", None)
    )

exl_info_db = exl_info[
    [
        "eigen_sku",
        "sku",
        "ean",
        "voorraad",
        "merk",
        "prijs",
        "advies_prijs",
        "category",
        "gewicht",
        "url_plaatje",
        "url_artikel",
        "product_title",
        "lange_omschrijving",
        "verpakings_eenheid",
    ]
]


huidige_datum = datetime.now().strftime("%d_%b_%Y")
exl_info_db.to_sql(f"{current_folder}_dag_{huidige_datum}", con=engine, if_exists="replace", index=False, chunksize=1000)

with engine.connect() as con:
    con.execute(f"ALTER TABLE {current_folder}_dag_{huidige_datum} ADD PRIMARY KEY (eigen_sku(20))")
    aantal_items = con.execute(f"SELECT count(*) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1]
    totaal_stock = int(con.execute(f"SELECT sum(voorraad) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1])
    totaal_prijs = int(con.execute(f"SELECT sum(prijs) FROM {current_folder}_dag_{huidige_datum}").fetchall()[-1][-1])
    leverancier = f"{current_folder}"
    sql_insert = (
        "INSERT INTO process_import_log (aantal_items, totaal_stock, totaal_prijs, leverancier) VALUES (%s,%s,%s,%s)"
    )
    con.execute(sql_insert, (aantal_items, totaal_stock, totaal_prijs, leverancier))

engine.dispose()
