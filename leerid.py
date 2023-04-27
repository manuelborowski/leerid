import pandas as pd
import requests, datetime, configparser, re
from zeep import Client
import logging, logging.handlers, os, sys

# V0.1: first version

version = "0.1"

stamnummer_cache = {}

config = configparser.ConfigParser()
config.read('config.ini')
log = logging.getLogger("leerid")
LOG_FILENAME = os.path.join(sys.path[0], f'log/leerid.txt')
log_level = getattr(logging, 'INFO')
log.setLevel(log_level)
log_handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1024 * 1024, backupCount=20)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
log_handler.setFormatter(log_formatter)
log.addHandler(log_handler)

log.info("START leerid")

dryrun = config["test"]["DRYRUN"] == "true"
ss_send_to = config["test"]["SS_MESSAGE_RECEIVER_ID"] if dryrun else ""

def get_leerlinggegevens_from_sdh():
    print("-> Lees gegevens van school-data-hub")
    global stamnummer_cache
    response = requests.get(config["default"]["SDH_API_URL"], headers={"x-api-key": config["default"]["SDH_API_KEY"]})
    response_json = response.json()
    if response_json["status"]:
        stamnummer_cache = {int(leerling["stamboeknummer"]): {"instellingsnummer": int(leerling["instellingsnummer"]),
                                                              "leerlingnummer": int(leerling["leerlingnummer"]),
                                                              "klascode": leerling["klascode"]} for leerling in response_json["data"]}
        print("--> SDH: gegevens zijn ok")
        log.info("Reading from SDH is OK")
        return True
    print(f"--> SDH: foutmelding: {response_json['data']}")
    log.error(f"Reading from SDH is NOK, {response_json['data']}")
    return False


def create_class_list():
    admingroep_cache = {}
    print("-> Maak een klassenlijst aan")
    if not get_leerlinggegevens_from_sdh():
        return False
    try:
        leerid_naam = input("--> LeerID invoer bestand: ")
        instellingsnummer = int(input("--> Instellingsnummer: "))
        df = pd.read_excel(leerid_naam)
        for i, row in df.iterrows():
            stamnummer = row["Stamnummer"]
            admingroep = row["Administratieve groep"]
            if stamnummer in stamnummer_cache and instellingsnummer == stamnummer_cache[stamnummer]["instellingsnummer"]:
                klascode = stamnummer_cache[stamnummer]["klascode"]

                if admingroep not in admingroep_cache:
                    admingroep_cache[admingroep] = set()
                admingroep_cache[admingroep].add(klascode)
        klaslijst = sorted([f"{', '.join( sorted(list(v)))} ({k})" for k, v in admingroep_cache.items()])
        with open(f"klassenlijst-{instellingsnummer}-{ datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.txt", "w") as klaslijst_file:
            klaslijst_file.write("# is commentaar, deze lijn wordt niet ingelezen.\n")
            klaslijst_file.write("# Om klassen te selecteren, verwijder de # aan het begin\n")
            klaslijst_file.write("# Na het verzenden wordt de lijn aangepast, bvb:\n")
            klaslijst_file.write("##1A (1e lj A) - 2023-04-01\n\n")
            klaslijst_file.write("###### START LIJST ######\n\n")
            for klas in klaslijst:
                klaslijst_file.write(f"#{klas}\n")
        print("-> Klassenlijst is klaar\n")
        log.info(f"Klassenlijst is ready, LeerID invoer {leerid_naam}, instellingsnummer {instellingsnummer}")
        return True
    except Exception as e:
        print(e)
        log.error(f"Could not create klassenlijst, {e}")
        return False


def send_leerid_to_students():
    print("-> Start met verzenden")
    if not get_leerlinggegevens_from_sdh():
        return False
    admingroup_lijst = []
    new_klaslijst = []
    now = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
    try:
        leerid_naam = input("-> Leerid invoer bestand: ")
        klaslijst_naam = input("-> Klassenlijst: ")
        with open(klaslijst_naam, "r") as klaslijst_file:
            for line in klaslijst_file:
                line = line.strip("\n")
                found = re.search("\(.*\)", line)
                if found and line.strip()[0] != "#":
                    admingroup = found[0][1:-1]
                    admingroup_lijst.append(admingroup)
                    line = f"##{line} - {now}"
                new_klaslijst.append(line)
        soap = Client(config["default"]["SS_API_URL"])
        api_key = config["default"]["SS_API_KEY"]
        send_from = config["default"]["SS_MESSAGE_SENDER_ID"]
        body_html = open("message-body.html").read()
        subject_text = open("message-subject.txt").read()
        df = pd.read_excel(leerid_naam)
        for i, row in df.iterrows():
            admingroep = row["Administratieve groep"]
            if admingroep in admingroup_lijst:
                send_to = ss_send_to if dryrun else stamnummer_cache[row["Stamnummer"]]["leerlingnummer"]
                body = body_html.replace("%%FIRSTNAME%%", row["Voornaam"])
                body = body.replace("%%USERNAME%%", row["LeerID Gebruikersnaam"])
                body = body.replace("%%PASSWORD%%", row["LeerID Wachtwoord"])
                print(f"--> {row['Achternaam']} {row['Voornaam']} krijgt login {row['LeerID Gebruikersnaam']}, verzonden naar {send_to}")
                ret = soap.service.sendMsg (api_key, send_to, subject_text, body, send_from, "", 0, False)
                print("Send returned:", ret)
                log.info(f"SensMsg, to {send_to}/{row['Achternaam']} {row['Voornaam']}, from {send_from}, username {row['LeerID Gebruikersnaam']}, password {row['LeerID Wachtwoord']}")

        print("-> LeerID gegevens zijn verzonden")
        log.info("SendMsg Done")
        with open(klaslijst_naam, "w") as klaslijst_file:
            for l in new_klaslijst:
                klaslijst_file.write(f"{l}\n")
        return True
    except Exception as e:
        print(e)
        log.error(f"Could not send credentials, {e}")
        return False


def show_info():
    print(f"""
        Versie: {version}
        Zorg dat je een excel hebt met de LeerID gegevens.
        Vanuit die excel kan je een klassenlijst genereren (zie menu).
        Pas die klassenlijst aan en selecteer de klassen waar je de LeerID gegevens naartoe wilt sturen.
        Zorg dat je een html-bestand hebt (messa-body.html) met de inhoud van het bericht dat je wilt sturen.
        Zorg dat je een tekst-bestand hebt (message-subject) met het onderwerp van het bericht dat je wilt sturen.
        Verzend de LeerID gegevens naar de geselecteerde klassen (zie menu).
    """)
    return True

menu_data = [
    ["Info", show_info],
    ["Maak klassenlijst aan", create_class_list],
    ["Verzend LeerID gegevens naar de leerlingen", send_leerid_to_students],
    ["Stop", None]
]

while True:
    for i, item in enumerate(menu_data):
        print(f"{i + 1}> {item[0]}")
    i += 1
    inp = input(f"Maak uw keuze (1-{i}): ")
    choice = int(inp)
    if choice >= 1 and choice < i:
        if not menu_data[choice - 1][1]():
            break
    if choice == i:
        break