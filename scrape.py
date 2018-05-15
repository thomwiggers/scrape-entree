import requests
import sqlite3


URL = 'https://www.entree.nu/Webservices/Verantwoording.asmx/GetVerantwoording'


def get_database():
    """Gets the database and creates it if necessary"""
    connection = sqlite3.connect('entree.db')
    connection.execute("""
        CREATE TABLE IF NOT EXISTS verantwoordingen (
            id INTEGER PRIMARY KEY,
            link TEXT,
            adres TEXT NOT NULL,
            inschrijfdatum TEXT NULL,
            volgorde TEXT NOT NULL,
            nr_reacties INTEGER NULL,
            positie INTEGER NULL,
            reden TEXT NULL,
            intrekreden TEXT NULL);
    """)
    return connection


def get_fresh_id(connection):
    res = connection.execute('SELECT MAX(id) from verantwoordingen').fetchone()
    if not res and res[0]:
        return 200000000
    else:
        id = res[0]
        if id < 200000000:
            return 200000000
        else:
            return id+1


def insert_entry(connection, entry):
    """Insert entries into the db"""
    data = {
        'link': entry['Link'],
        'adres': entry['Adres'],
        'inschrijfdatum': entry['InschrijfDatum'],
        'volgorde': entry['Volgorde'],
        'nr_reacties': entry['NrReacties'],
        'positie': entry['Positie'],
        'reden': entry['Reden'],
        'intrekreden': entry['IntrekReden'],
    }
    has = connection.execute('SELECT 1 FROM verantwoordingen WHERE adres=?',
                             (data['adres'],)).fetchone()
    if data['link']:
        data['id'] = int(
            entry['Link'][len('https://www.entree.nu/detail?ID='):])
    elif not has:
        print("Generating an ID for an entry without a link")
        data['id'] = get_fresh_id(connection)

    if not has:
        print("Inserting", data['adres'])
        connection.execute(
            """INSERT INTO verantwoordingen (
                id, link, adres, inschrijfdatum, volgorde, nr_reacties,
                positie, reden, intrekreden) VALUES (
                :id, :link, :adres, :inschrijfdatum, :volgorde, :nr_reacties,
                :positie, :reden, :intrekreden)""",
            data)
        connection.commit()


def get_entree_entries():
    """Get the entries from entree"""
    data = {
        'pagenumber': 0,
        'aantal': 1,
        'sortering': '',
    }
    entries = requests.post(URL, json=data).json()['d']

    database = get_database()
    for entry in entries:
        insert_entry(database, entry)


if __name__ == "__main__":
    get_entree_entries()
